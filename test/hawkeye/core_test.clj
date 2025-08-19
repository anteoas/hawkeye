(ns hawkeye.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [hawkeye.core :as hawk])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; Wait times based on platform
(def ^:private event-wait-time
  "Time to wait for events - longer on macOS due to WatchService delays"
  (if (#'hawk/mac-osx?) 2500 200))

(def ^:private startup-wait-time
  "Time to wait for watcher startup"
  (if (#'hawk/mac-osx?) 200 100))

;; Test helpers
(defn create-temp-dir
  "Create a temporary directory for testing"
  []
  (str (Files/createTempDirectory "hawk-test" (make-array FileAttribute 0))))

(defn create-temp-file
  "Create a temporary file in the given directory"
  [dir name content]
  (let [file (io/file dir name)]
    (spit file content)
    file))

(defn delete-recursively
  "Delete a directory and all its contents"
  [path]
  (let [file (io/file path)]
    (when (.exists file)
      (if (.isDirectory file)
        (doseq [child (.listFiles file)]
          (delete-recursively child)))
      (.delete file))))

;; Debounce tests
(deftest test-debounce-last
  (testing "debounce with :last (default) mode"
    (let [calls (atom [])
          f (fn [& args] (swap! calls conj args))
          df (hawk/debounce f 50)]

      ;; Make rapid calls
      (df "A")
      (Thread/sleep 10)
      (df "B")
      (Thread/sleep 10)
      (df "C")

      ;; Wait for debounce
      (Thread/sleep startup-wait-time)

      ;; Should only have last call
      (is (= [["C"]] @calls)))))

(deftest test-debounce-first
  (testing "debounce with :first mode"
    (let [calls (atom [])
          f (fn [& args] (swap! calls conj args))
          df (hawk/debounce f 50 :events :first)]

      ;; Make rapid calls
      (df "A")
      (Thread/sleep 10)
      (df "B")
      (Thread/sleep 10)
      (df "C")

      ;; Wait for debounce
      (Thread/sleep startup-wait-time)

      ;; Should only have first call
      (is (= [["A"]] @calls)))))

(deftest test-debounce-all
  (testing "debounce with :all mode"
    (let [calls (atom [])
          f (fn [args] (swap! calls conj args))
          df (hawk/debounce f 50 :events :all)]

      ;; Make rapid calls
      (df "A")
      (Thread/sleep 10)
      (df "B")
      (Thread/sleep 10)
      (df "C")

      ;; Wait for debounce
      (Thread/sleep startup-wait-time)

      ;; Should have vector of all calls
      (is (= [[["A"] ["B"] ["C"]]] @calls)))))

(deftest test-debounce-unique
  (testing "debounce with :unique mode"
    (let [calls (atom [])
          f (fn [args] (swap! calls conj args))
          df (hawk/debounce f 50 :events :unique)]

      ;; Make rapid calls with duplicates
      (df "A")
      (Thread/sleep 10)
      (df "B")
      (Thread/sleep 10)
      (df "A") ; duplicate
      (Thread/sleep 10)
      (df "C")

      ;; Wait for debounce
      (Thread/sleep startup-wait-time)

      ;; Should have vector of unique calls
      (is (= [[["A"] ["B"] ["C"]]] @calls)))))

(deftest test-debounce-multiple-batches
  (testing "debounce handles multiple batches correctly"
    (let [calls (atom [])
          f (fn [& args] (swap! calls conj args))
          df (hawk/debounce f 50)]

      ;; First batch
      (df 1)
      (df 2)
      (df 3)

      ;; Wait for first batch
      (Thread/sleep startup-wait-time)

      ;; Second batch
      (df 4)
      (df 5)

      ;; Wait for second batch
      (Thread/sleep startup-wait-time)

      ;; Should have two calls with last value from each batch
      (is (= [[3] [5]] @calls)))))

;; Private function tests
(deftest test-find-all-directories
  (testing "find-all-directories returns all subdirectories"
    ;; Create a temp directory structure
    (let [temp-dir (Files/createTempDirectory
                    "test"
                    (make-array FileAttribute 0))
          sub1 (Files/createDirectory
                (.resolve temp-dir "sub1")
                (make-array FileAttribute 0))
          sub2 (Files/createDirectory
                (.resolve temp-dir "sub2")
                (make-array FileAttribute 0))
          subsub (Files/createDirectory
                  (.resolve sub1 "subsub")
                  (make-array FileAttribute 0))]

      (try
        (let [dirs (set (#'hawk/find-all-directories (str temp-dir)))]
          ;; Should find all directories
          (is (contains? dirs (str temp-dir)))
          (is (contains? dirs (str sub1)))
          (is (contains? dirs (str sub2)))
          (is (contains? dirs (str subsub)))
          (is (= 4 (count dirs))))

        ;; Cleanup
        (finally
          (Files/delete subsub)
          (Files/delete sub1)
          (Files/delete sub2)
          (Files/delete temp-dir))))))

(deftest test-find-all-directories-nonexistent
  (testing "find-all-directories returns empty for nonexistent path"
    (is (= [] (#'hawk/find-all-directories "/nonexistent/path/that/does/not/exist")))))

(deftest test-find-all-directories-file
  (testing "find-all-directories returns empty for file path"
    (let [temp-file (Files/createTempFile
                     "test" ".txt"
                     (make-array FileAttribute 0))]
      (try
        (is (= [] (#'hawk/find-all-directories (str temp-file))))
        (finally
          (Files/delete temp-file))))))

;; File watching tests
(deftest test-watch-file-creation
  (testing "watch detects file creation"
    (let [temp-dir (create-temp-dir)
          events (atom [])
          errors (atom [])
          stop (hawk/watch [temp-dir]
                           (fn [e] (swap! events conj e))
                           (fn [e _] (swap! errors conj e)))]
      (try
        ;; Create a file
        (Thread/sleep startup-wait-time) ; Let watcher start
        (create-temp-file temp-dir "test.txt" "content")
        (Thread/sleep event-wait-time) ; Wait for event

        ;; Should have creation event
        (is (seq @events))
        (is (= :create (:type (first @events))))
        (is (= "test.txt" (:file (first @events))))
        (is (empty? @errors))

        (finally
          (stop)
          (delete-recursively temp-dir))))))

(deftest test-watch-single-string-path
  (testing "watch accepts a single string path"
    (let [temp-dir (create-temp-dir)
          events (atom [])
          errors (atom [])
          ;; Pass a single string instead of a collection
          stop (hawk/watch temp-dir
                           (fn [e] (swap! events conj e))
                           (fn [e _] (swap! errors conj e)))]
      (try
        ;; Create a file
        (Thread/sleep startup-wait-time)
        (create-temp-file temp-dir "test.txt" "content")
        (Thread/sleep event-wait-time)

        ;; Should have creation event
        (is (seq @events))
        (is (= :create (:type (first @events))))
        (is (= "test.txt" (:file (first @events))))
        (is (empty? @errors))

        (finally
          (stop)
          (delete-recursively temp-dir))))))

(deftest test-watch-file-modification
  (testing "watch detects file modification"
    ;; NOTE: This test may fail with FSEvents if the file exists before watching starts
    ;; FSEvents may report it as a create event instead of modify
    (let [temp-dir (create-temp-dir)
          test-file (create-temp-file temp-dir "test.txt" "initial")
          events (atom [])
          errors (atom [])
          stop (hawk/watch [temp-dir]
                           (fn [e] (swap! events conj e))
                           (fn [e _] (swap! errors conj e)))]
      (try
        ;; Modify the file
        (Thread/sleep startup-wait-time) ; Let watcher start
        (spit test-file "modified content")
        (Thread/sleep event-wait-time) ; Wait for event

        ;; Should have modification event
        (is (seq @events))
        (let [modify-events (filter #(= :modify (:type %)) @events)]
          (is (seq modify-events))
          (is (= "test.txt" (:file (first modify-events)))))
        (is (empty? @errors))

        (finally
          (stop)
          (delete-recursively temp-dir))))))

(deftest test-watch-file-deletion
  (testing "watch detects file deletion"
    (let [temp-dir (create-temp-dir)
          test-file (create-temp-file temp-dir "test.txt" "content")
          events (atom [])
          errors (atom [])
          stop (hawk/watch [temp-dir]
                           (fn [e] (swap! events conj e))
                           (fn [e _] (swap! errors conj e)))]
      (try
        ;; Delete the file
        (Thread/sleep startup-wait-time) ; Let watcher start
        (.delete test-file)
        (Thread/sleep event-wait-time) ; Wait for event

        ;; Should have deletion event
        (is (seq @events))
        (let [delete-events (filter #(= :delete (:type %)) @events)]
          (is (seq delete-events))
          (is (= "test.txt" (:file (first delete-events)))))
        (is (empty? @errors))

        (finally
          (stop)
          (delete-recursively temp-dir))))))

(deftest test-watch-multiple-directories
  (testing "watch monitors multiple directories"
    (let [temp-dir1 (create-temp-dir)
          temp-dir2 (create-temp-dir)
          events (atom [])
          errors (atom [])
          stop (hawk/watch [temp-dir1 temp-dir2]
                           (fn [e] (swap! events conj e))
                           (fn [e _] (swap! errors conj e)))]
      (try
        ;; Create files in both directories
        (Thread/sleep startup-wait-time) ; Let watcher start
        (create-temp-file temp-dir1 "file1.txt" "content1")
        (create-temp-file temp-dir2 "file2.txt" "content2")
        (Thread/sleep event-wait-time) ; Wait for events

        ;; Should have events from both directories
        (is (>= (count @events) 2))
        (let [files (set (map :file @events))]
          (is (contains? files "file1.txt"))
          (is (contains? files "file2.txt")))
        (is (empty? @errors))

        (finally
          (stop)
          (delete-recursively temp-dir1)
          (delete-recursively temp-dir2))))))

(deftest test-watch-subdirectory-creation
  (testing "watch automatically monitors new subdirectories"
    (let [temp-dir (create-temp-dir)
          events (atom [])
          errors (atom [])
          stop (hawk/watch [temp-dir]
                           (fn [e] (swap! events conj e))
                           (fn [e _] (swap! errors conj e)))]
      (try
        ;; Create subdirectory
        (Thread/sleep startup-wait-time) ; Let watcher start
        (let [sub-dir (io/file temp-dir "subdir")]
          (.mkdir sub-dir)
          (Thread/sleep event-wait-time) ; Wait for directory creation event

          ;; Create file in subdirectory
          (create-temp-file sub-dir "nested.txt" "content")
          (Thread/sleep event-wait-time)) ; Wait for file event

        ;; Should have events for both directory and file in subdirectory
        (is (seq @events))
        (let [nested-events (filter #(re-find #"nested\.txt" (:path %)) @events)]
          (is (seq nested-events)))
        (is (empty? @errors))

        (finally
          (stop)
          (delete-recursively temp-dir))))))

(deftest test-watch-error-handling
  (testing "watch handles errors gracefully"
    (let [errors (atom [])
          events (atom [])
          ;; Try to watch non-existent directory
          stop (hawk/watch ["/non/existent/path"]
                           (fn [e] (swap! events conj e))
                           (fn [e ctx] (swap! errors conj {:error e :context ctx})))]
      (try
        (Thread/sleep startup-wait-time)
        ;; Should not crash, errors should be reported via error-fn
        (is (empty? @events))
        (finally
          (stop))))))

(deftest test-stop-function
  (testing "stop function properly shuts down watcher"
    ;; NOTE: This test will fail on macOS with :auto mode due to FSEvents limitation
    ;; where events continue after stop. See docs/fsevents-investigation.md
    (let [temp-dir (create-temp-dir)
          events (atom [])
          errors (atom [])
          stop (hawk/watch [temp-dir]
                           (fn [e] (swap! events conj e))
                           (fn [e _] (swap! errors conj e)))]
      (try
        ;; Stop the watcher
        (Thread/sleep startup-wait-time)
        (stop)

        ;; Create file after stopping
        (Thread/sleep startup-wait-time)
        (reset! events [])
        (create-temp-file temp-dir "after-stop.txt" "content")
        (Thread/sleep event-wait-time)

        ;; Should not receive events after stopping
        (is (empty? @events))

        (finally
          (delete-recursively temp-dir))))))

(deftest test-stop-idempotent
  (testing "stop function can be called multiple times safely"
    (let [temp-dir (create-temp-dir)
          events (atom [])
          errors (atom [])
          stop (hawk/watch [temp-dir]
                           (fn [e] (swap! events conj e))
                           (fn [e _] (swap! errors conj e))
                           {:mode :poll :poll-ms 50})] ; Use explicit poll mode with faster polling
      (try
        ;; Let watcher start
        (Thread/sleep startup-wait-time)

        ;; Stop once
        (stop)

        ;; Give time for stop to take effect
        (Thread/sleep 100)

        ;; Stop again - should not throw
        (stop)

        ;; And again - still safe
        (stop)

        ;; Create file after stopping
        (create-temp-file temp-dir "after-stop.txt" "content")
        (Thread/sleep event-wait-time)

        ;; Should not receive any events
        (is (empty? @events))
        (is (empty? @errors))

        (finally
          (delete-recursively temp-dir))))))

(deftest test-stop-idempotent-all-modes
  (testing "stop function is idempotent for all watch modes"
    ;; NOTE: The "no events after stop" assertion will fail for :fsevents mode
    ;; due to known FSEvents limitation. See docs/fsevents-investigation.md
    (let [temp-dir (create-temp-dir)]
      (try
        ;; Test each mode that's available on this platform
        (doseq [mode (cond-> [:poll]
                       (#'hawk/virtual-threads-available?) (conj :vthread)
                       (#'hawk/mac-osx?) (conj :fsevents))]
          (testing (str "Mode: " mode)
            (let [events (atom [])
                  errors (atom [])
                  stop (hawk/watch [temp-dir]
                                   (fn [e] (swap! events conj e))
                                   (fn [e _] (swap! errors conj e))
                                   {:mode mode})]
              (try
                ;; Verify correct mode was used
                (is (= mode (:hawkeye/mode (meta stop))))

                ;; Let watcher start
                (Thread/sleep startup-wait-time)

                ;; Create a file to ensure watcher is working
                (create-temp-file temp-dir (str "test-" (name mode) ".txt") "content")
                (Thread/sleep event-wait-time)

                ;; Should have received the create event
                (is (seq @events) "Should have received create event")

                ;; Now test idempotent stop
                (reset! events [])
                (reset! errors [])

                ;; Stop once
                (stop)

                ;; Stop again - should not throw
                (stop)

                ;; And again - still safe
                (stop)

                ;; Create file after stopping
                (create-temp-file temp-dir (str "after-stop-" (name mode) ".txt") "content")
                (Thread/sleep event-wait-time)

                ;; Should not receive any new events
                (is (empty? @events) "Should not receive events after stop")
                (is (empty? @errors) "Should not have any errors from multiple stops")

                (finally
                  ;; Ensure stopped even if test fails
                  (try (stop) (catch Exception _)))))))

        (finally
          (delete-recursively temp-dir))))))

(deftest test-stop-behaviors-per-implementation
  (testing "stop behavior specifics for each implementation"
    (let [temp-dir (create-temp-dir)]
      (try
        ;; Test PollingWatcher stop behavior
        (testing "PollingWatcher stop behavior"
          (let [events (atom [])
                errors (atom [])
                stop (hawk/watch [temp-dir]
                                 (fn [e] (swap! events conj e))
                                 (fn [e _] (swap! errors conj e))
                                 {:mode :poll :poll-ms 50})]
            (try
              (Thread/sleep startup-wait-time)

              ;; Stop should cancel the future and close the watcher
              (stop)

              ;; Multiple stops should be safe
              (stop)
              (stop)

              ;; No errors should occur
              (is (empty? @errors))

              (finally
                (try (stop) (catch Exception _))))))

        ;; Test VirtualThreadWatcher stop behavior (if available)
        (when (#'hawk/virtual-threads-available?)
          (testing "VirtualThreadWatcher stop behavior"
            (let [events (atom [])
                  errors (atom [])
                  stop (hawk/watch [temp-dir]
                                   (fn [e] (swap! events conj e))
                                   (fn [e _] (swap! errors conj e))
                                   {:mode :vthread})]
              (try
                (Thread/sleep startup-wait-time)

                ;; Stop should close watcher and interrupt thread
                (stop)

                ;; Multiple stops should be safe due to compare-and-set
                (stop)
                (stop)

                ;; No errors should occur
                (is (empty? @errors))

                (finally
                  (try (stop) (catch Exception _)))))))

        ;; Test FSEventsWatcher stop behavior (macOS only)
        (when (#'hawk/mac-osx?)
          (testing "FSEventsWatcher stop behavior"
            (let [events (atom [])
                  errors (atom [])
                  ;; Use explicit fsevents mode
                  stop (try
                         (hawk/watch [temp-dir]
                                     (fn [e] (swap! events conj e))
                                     (fn [e _] (swap! errors conj e))
                                     {:mode :fsevents})
                         (catch Exception e
                           ;; If FSEvents fails to initialize, skip this test
                           (println "Skipping FSEvents test:" (.getMessage e))
                           nil))]
              (when stop
                (try
                  ;; Verify we got FSEvents mode
                  (is (= :fsevents (:hawkeye/mode (meta stop))))

                  (Thread/sleep startup-wait-time)

                  ;; First stop should stop the monitor
                  (stop)

                  ;; Subsequent stops should be safe due to swap!
                  (stop)
                  (stop)

                  ;; No errors should occur
                  (is (empty? @errors))

                  (finally
                    (try (stop) (catch Exception _))))))))

        (finally
          (delete-recursively temp-dir))))))

(deftest test-debounced-watch
  (testing "using debounce with watch"
    ;; NOTE: This test is timing-sensitive and may fail if the system is under load
    ;; or if file system events are delayed
    (let [temp-dir (create-temp-dir)
          rebuild-calls (atom [])
          debounced-rebuild (hawk/debounce
                             (fn [e] (swap! rebuild-calls conj e))
                             100)
          errors (atom [])
          stop (hawk/watch [temp-dir]
                           debounced-rebuild
                           (fn [e _] (swap! errors conj e)))]
      (try
        ;; Create multiple files rapidly
        (Thread/sleep startup-wait-time) ; Let watcher start
        (create-temp-file temp-dir "file1.txt" "content")
        (create-temp-file temp-dir "file2.txt" "content")
        (create-temp-file temp-dir "file3.txt" "content")
        (Thread/sleep 300) ; Wait for debounce

        ;; Should only have one rebuild call due to debounce
        (is (= 1 (count @rebuild-calls)))
        (is (empty? @errors))

        (finally
          (stop)
          (delete-recursively temp-dir))))))

;; Mode tests
(deftest test-watch-modes
  (testing "watch returns correct mode in metadata"
    (let [temp-dir (create-temp-dir)]
      (try
        ;; Test auto mode
        (let [stop (hawk/watch [temp-dir]
                               (fn [_])
                               (fn [_ _]))]
          (try
            (let [mode (:hawkeye/mode (meta stop))]
              (is (contains? #{:poll :vthread :fsevents} mode)))
            (finally (stop))))

        ;; Test explicit poll mode
        (let [stop (hawk/watch [temp-dir]
                               (fn [_])
                               (fn [_ _])
                               {:mode :poll})]
          (try
            (is (= :poll (:hawkeye/mode (meta stop))))
            (finally (stop))))

        ;; Test explicit vthread mode if available
        (when (#'hawk/virtual-threads-available?)
          (let [stop (hawk/watch [temp-dir]
                                 (fn [_])
                                 (fn [_ _])
                                 {:mode :vthread})]
            (try
              (is (= :vthread (:hawkeye/mode (meta stop))))
              (finally (stop)))))

        (finally
          (delete-recursively temp-dir))))))

(deftest test-event-structure
  (testing "events have correct structure"
    (let [temp-dir (create-temp-dir)
          events (atom [])
          stop (hawk/watch [temp-dir]
                           (fn [e] (swap! events conj e))
                           (fn [_ _]))]
      (try
        (Thread/sleep startup-wait-time)
        (create-temp-file temp-dir "test.txt" "content")
        (Thread/sleep event-wait-time)

        (is (seq @events))
        (let [event (first @events)]
          ;; Check event structure
          (is (contains? event :type))
          (is (contains? event :file))
          (is (contains? event :path))
          (is (contains? event :timestamp))

          ;; Check event values
          (is (keyword? (:type event)))
          (is (string? (:file event)))
          (is (string? (:path event)))
          (is (number? (:timestamp event))))

        (finally
          (stop)
          (delete-recursively temp-dir))))))

(deftest test-recursive-watching
  (testing "watch recursively monitors all subdirectories by default"
    (let [temp-dir (create-temp-dir)
          events (atom [])
          errors (atom [])]

      ;; Create nested directory structure before watching
      (let [sub1 (io/file temp-dir "sub1")
            sub2 (io/file sub1 "sub2")
            sub3 (io/file sub2 "sub3")]
        (.mkdirs sub3)

        (let [stop (hawk/watch [temp-dir]
                               (fn [e] (swap! events conj e))
                               (fn [e _] (swap! errors conj e)))]
          (try
            (Thread/sleep startup-wait-time)

            ;; Create files at different levels
            (create-temp-file temp-dir "root.txt" "content")
            (create-temp-file sub1 "level1.txt" "content")
            (create-temp-file sub2 "level2.txt" "content")
            (create-temp-file sub3 "level3.txt" "content")

            (Thread/sleep (if (#'hawk/mac-osx?) 2500 500)) ; Give time for all events

            ;; Should have events from all levels
            (let [files (set (map :file @events))]
              (is (contains? files "root.txt"))
              (is (contains? files "level1.txt"))
              (is (contains? files "level2.txt"))
              (is (contains? files "level3.txt")))

            (is (empty? @errors))

            (finally
              (stop)
              (delete-recursively temp-dir))))))))

;; Platform-specific tests
(when (#'hawk/mac-osx?)
  (deftest test-fsevents-mode
    (testing "FSEvents mode is used on macOS when available"
      (let [temp-dir (create-temp-dir)]
        (try
          ;; When JNA is available, should use FSEvents
          (let [stop (hawk/watch [temp-dir]
                                 (fn [_])
                                 (fn [_ _])
                                 {:mode :auto})]
            (try
              ;; On macOS with JNA, should be either fsevents or poll (if JNA failed)
              (is (contains? #{:fsevents :poll} (:hawkeye/mode (meta stop))))
              (finally (stop))))

          (finally
            (delete-recursively temp-dir)))))))

(deftest test-poll-interval
  (testing "poll mode respects poll-ms option"
    (let [temp-dir (create-temp-dir)
          events (atom [])
          stop (hawk/watch [temp-dir]
                           (fn [e] (swap! events conj e))
                           (fn [_ _])
                           {:mode :poll
                            :poll-ms 50})] ; Faster polling
      (try
        (Thread/sleep startup-wait-time)
        (create-temp-file temp-dir "test.txt" "content")
        (Thread/sleep (if (#'hawk/mac-osx?) 2500 150)) ; macOS needs longer even with poll-ms

        (is (seq @events))

        (finally
          (stop)
          (delete-recursively temp-dir))))))