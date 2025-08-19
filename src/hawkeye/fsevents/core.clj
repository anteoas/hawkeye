(ns hawkeye.fsevents.core
  "JNA-based FSEvents monitor for macOS.
   
   A lightweight file system events monitor using JNA to interface with
   macOS's FSEvents API. Monitors directories for file changes including
   creation, modification, deletion, and renaming.
   
   Known Limitation: Uses deprecated CFRunLoop APIs. Events may continue
   to be delivered after stop() is called. See docs/fsevents-investigation.md
   for details."
  (:import [com.sun.jna Native Pointer Memory]))

;; Load native libraries
(def ^:private cf-lib (com.sun.jna.NativeLibrary/getInstance "CoreFoundation"))
(def ^:private cs-lib (com.sun.jna.NativeLibrary/getInstance "CoreServices"))

;; Constants
(def ^:private kCFAllocatorDefault nil)
(def ^:private kCFStringEncodingUTF8 0x08000100)
(def ^:private kFSEventStreamEventIdSinceNow -1)
(def ^:private kFSEventStreamCreateFlagFileEvents 0x10)

;; Event flags
(def event-flags
  "FSEvents flag constants mapped to keywords"
  {:created 0x100
   :removed 0x200
   :modified 0x1000
   :renamed 0x800
   :is-file 0x10000
   :is-dir 0x20000})

;; Native functions
(def ^:private CFStringCreateWithCString
  (.getFunction cf-lib "CFStringCreateWithCString"))
(def ^:private CFArrayCreate
  (.getFunction cf-lib "CFArrayCreate"))
(def ^:private CFRelease
  (.getFunction cf-lib "CFRelease"))
(def ^:private FSEventStreamCreate
  (.getFunction cs-lib "FSEventStreamCreate"))
(def ^:private FSEventStreamStart
  (.getFunction cs-lib "FSEventStreamStart"))
(def ^:private FSEventStreamStop
  (.getFunction cs-lib "FSEventStreamStop"))
(def ^:private FSEventStreamInvalidate
  (.getFunction cs-lib "FSEventStreamInvalidate"))
(def ^:private FSEventStreamRelease
  (.getFunction cs-lib "FSEventStreamRelease"))
(def ^:private FSEventStreamScheduleWithRunLoop
  (.getFunction cs-lib "FSEventStreamScheduleWithRunLoop"))
(def ^:private FSEventStreamUnscheduleFromRunLoop
  (.getFunction cs-lib "FSEventStreamUnscheduleFromRunLoop"))
(def ^:private CFRunLoopGetCurrent
  (.getFunction cf-lib "CFRunLoopGetCurrent"))
(def ^:private CFRunLoopRun
  (.getFunction cf-lib "CFRunLoopRun"))
(def ^:private CFRunLoopStop
  (.getFunction cf-lib "CFRunLoopStop"))

(def ^:private kCFRunLoopDefaultMode
  (let [sym (.getGlobalVariableAddress cf-lib "kCFRunLoopDefaultMode")]
    (.getPointer sym 0)))

(defn- decode-flags
  "Convert FSEvents flag bits to a set of keywords"
  [flags]
  (reduce-kv (fn [acc k v]
               (if (pos? (bit-and flags v))
                 (conj acc k)
                 acc))
             #{}
             event-flags))

(defn- create-callback
  "Create a JNA callback for FSEvents"
  [handler stream-atom]
  (reify hawkeye.fsevents.FSEventCallback
    (callback [_ _ _ numEvents eventPaths eventFlags eventIds]
      ;; Only process events if we haven't been stopped
      (when @stream-atom
        (try
          (let [paths-ptr eventPaths
                num-events (int numEvents)]
            (dotimes [i num-events]
              (try
                (let [path-ptr (.getPointer paths-ptr (* i (Native/POINTER_SIZE)))
                      ;; Defensive: check for null pointer before string access
                      path (when (and path-ptr (not (.equals path-ptr Pointer/NULL)))
                             (.getString path-ptr 0))
                      flag (when eventFlags
                             (.getInt eventFlags (* i 4)))
                      event-id (when eventIds
                                 (.getLong eventIds (* i 8)))]
                  ;; Only process if we got valid data and haven't been stopped
                  (when (and path flag event-id @stream-atom)
                    (handler {:path path
                              :event-id event-id
                              :flags flag
                              :flag-names (decode-flags flag)})))
                (catch Exception e
                  ;; Log but don't crash on individual event errors
                  (println "Error processing FSEvent:" (.getMessage e))))))
          (catch Exception e
            ;; Log but don't crash on callback errors
            (println "Error in FSEvents callback:" (.getMessage e))))))))

(defn watch
  "Start watching directories for file system events.
   
   Parameters:
   - paths: Collection of directory paths to watch (strings)
   - handler: Function called with event maps containing:
     - :path - The file path that changed
     - :event-id - Unique event identifier
     - :flags - Raw event flags
     - :flag-names - Set of keywords (:created, :modified, :removed, etc.)
   
   Returns a monitor object that should be passed to `stop` to cease monitoring."
  [paths handler]
  ;; Check for empty paths
  (when (empty? paths)
    (throw (IllegalArgumentException. "No paths provided to watch")))

  (let [;; Create CFStrings for all paths
        cf-paths-vec (mapv (fn [path]
                             (let [cf-str (.invoke CFStringCreateWithCString
                                                   Pointer
                                                   (to-array [kCFAllocatorDefault path (long kCFStringEncodingUTF8)]))]
                               (when (nil? cf-str)
                                 (throw (Exception. (str "Failed to create CFString for path: " path))))
                               cf-str))
                           paths)

        ;; Create array of path pointers
        paths-array (doto (Memory. (* (count cf-paths-vec) Native/POINTER_SIZE))
                      (as-> mem
                            (doseq [[idx cf-path] (map-indexed vector cf-paths-vec)]
                              (.setPointer mem (* idx Native/POINTER_SIZE) cf-path))))

        ;; Create CFArray with all paths
        cf-paths (.invoke CFArrayCreate
                          Pointer
                          (to-array [kCFAllocatorDefault paths-array (count cf-paths-vec) nil]))]

    (when (nil? cf-paths)
      (doseq [cf-path cf-paths-vec]
        (.invoke CFRelease Void/TYPE (to-array [cf-path])))
      (throw (Exception. "Failed to create CFArray")))

    (let [;; Create stream-atom early so callback can reference it
          stream-atom (atom nil)
          ;; Create callback and event stream
          callback (create-callback handler stream-atom)
          stream (.invoke FSEventStreamCreate
                          Pointer
                          (to-array [kCFAllocatorDefault
                                     callback
                                     nil
                                     cf-paths
                                     (long kFSEventStreamEventIdSinceNow)
                                     (double 0.0)
                                     (int kFSEventStreamCreateFlagFileEvents)]))]

      (when (nil? stream)
        (.invoke CFRelease Void/TYPE (to-array [cf-paths]))
        (doseq [cf-path cf-paths-vec]
          (.invoke CFRelease Void/TYPE (to-array [cf-path])))
        (throw (Exception. "Failed to create FSEventStream")))

      ;; Set the stream in the atom now that we have it
      (reset! stream-atom stream)

      (let [run-loop (.invoke CFRunLoopGetCurrent Pointer (to-array []))]

        ;; Schedule the stream
        (.invoke FSEventStreamScheduleWithRunLoop
                 Void/TYPE
                 (to-array [stream run-loop kCFRunLoopDefaultMode]))

        ;; Start the stream
        (let [started (.invoke FSEventStreamStart Boolean (to-array [stream]))]
          (if started
            {:stream stream
             :stream-atom stream-atom ; Use the existing atom
             :run-loop run-loop
             :cf-paths-vec cf-paths-vec
             :cf-paths cf-paths
             :callback callback
             :thread (Thread/currentThread)}

            (do
              (.invoke FSEventStreamRelease Void/TYPE (to-array [stream]))
              (.invoke CFRelease Void/TYPE (to-array [cf-paths]))
              (doseq [cf-path cf-paths-vec]
                (.invoke CFRelease Void/TYPE (to-array [cf-path])))
              (throw (Exception. "Failed to start FSEventStream")))))))))

(defn stop
  "Stop watching directories.
   
   Parameters:
   - monitor: The monitor object returned by `watch`
   
   Known Limitation:
   Due to the nature of CFRunLoop, events may continue to be delivered
   after this function returns. The background thread will persist.
   See docs/fsevents-investigation.md for detailed analysis."
  [monitor]
  (when monitor
    ;; Use the stream-atom to ensure idempotent stop
    (when-let [stream-atom (:stream-atom monitor)]
      ;; Get the current stream value
      (let [stream @stream-atom]
        (when stream
          ;; Try to atomically set it to nil - only proceed if successful
          (when (compare-and-set! stream-atom stream nil)
            ;; Stop the stream first
            (try
              (.invoke FSEventStreamStop Void/TYPE (to-array [stream]))
              (catch Exception _ nil))

            ;; Unschedule from run loop
            (when-let [run-loop (:run-loop monitor)]
              (try
                (.invoke FSEventStreamUnscheduleFromRunLoop
                         Void/TYPE
                         (to-array [stream run-loop kCFRunLoopDefaultMode]))
                (catch Exception _ nil)))

            ;; Invalidate the stream
            (try
              (.invoke FSEventStreamInvalidate Void/TYPE (to-array [stream]))
              (catch Exception _ nil))

            ;; Now stop the run loop
            (when-let [run-loop (:run-loop monitor)]
              (try
                (.invoke CFRunLoopStop Void/TYPE (to-array [run-loop]))
                (catch Exception _ nil)))

            ;; Give time for the run loop to exit
            (Thread/sleep 100)

            ;; Release the stream
            (try
              (.invoke FSEventStreamRelease Void/TYPE (to-array [stream]))
              (catch Exception _ nil))

            ;; Release other resources in reverse order
            (when-let [cf-paths (:cf-paths monitor)]
              (try
                (.invoke CFRelease Void/TYPE (to-array [cf-paths]))
                (catch Exception _ nil)))

            (when-let [cf-paths-vec (:cf-paths-vec monitor)]
              (doseq [cf-path (reverse cf-paths-vec)]
                (try
                  (.invoke CFRelease Void/TYPE (to-array [cf-path]))
                  (catch Exception _ nil))))

            ;; Wait for the runner thread to complete if async
            (when-let [runner-future (:runner-future monitor)]
              (try
                ;; Give it up to 1 second to complete
                (let [result (deref runner-future 1000 ::timeout)]
                  (when (= result ::timeout)
                    ;; If it didn't complete, try to interrupt it
                    (future-cancel runner-future)))
                (catch Exception _ nil)))))))))

(defn run-loop
  "Run the event loop for a monitor. Blocks until the monitor is stopped.
   
   Parameters:
   - monitor: The monitor object returned by `watch`"
  [monitor]
  ;; Check if we should still be running before entering the loop
  (when (and (:stream-atom monitor) @(:stream-atom monitor))
    (.invoke CFRunLoopRun Void/TYPE (to-array []))))

(defn watch-async
  "Start watching directories in a background thread.
   
   Same as `watch` but runs the event loop in a separate thread,
   returning immediately.
   
   Parameters:
   - paths: Collection of directory paths to watch
   - handler: Event handler function
   
   Returns a monitor object that should be passed to `stop`."
  [paths handler]
  (let [monitor-promise (promise)
        ;; Store the future so we can wait for it to complete
        runner-future (future
                        (try
                          (let [monitor (watch paths handler)]
                            (deliver monitor-promise monitor)
                            (run-loop monitor))
                          (catch Exception e
                            (deliver monitor-promise e))))]
    (let [result @monitor-promise]
      (if (instance? Exception result)
        (throw result)
        ;; Add the future to the monitor so stop can wait for it
        (assoc result :runner-future runner-future)))))
