(ns hawkeye.fsevents.monitor
  "FSEvents integration for hawkeye."
  (:require [hawkeye.fsevents.core :as fs]
            [clojure.java.io :as io]))

(defn- normalize-event
  "Convert FSEvents event to hawkeye format.
   Uses known-files atom to track whether files existed before."
  [event known-files]
  (let [path (:path event)
        file (io/file path)
        flags (:flag-names event)
        file-path (.getAbsolutePath file)]
    ;; Skip directory events - only process files
    (when (or (.isFile file)
              ;; For delete events, we can't check if it's a file, so check flags
              (and (contains? flags :removed)
                   (not (contains? flags :is-dir))))
      ;; Determine the event type
      (let [event-type (cond
                         ;; Removed is always delete
                         (contains? flags :removed) :delete

                         ;; For modified/created flags, check if we've seen this file before
                         (contains? flags :modified)
                         (if (contains? @known-files file-path)
                           :modify
                           :create)

                         ;; Created flag
                         (contains? flags :created) :create

                         ;; Shouldn't happen, but default to modify
                         :else :modify)]

        ;; Update known files based on event type
        (case event-type
          :create (swap! known-files conj file-path)
          :delete (swap! known-files disj file-path)
          nil)

        ;; Return normalized event
        {:type event-type
         :file (.getName file)
         :path path
         :timestamp (System/currentTimeMillis)}))))

(defn watch-paths
  "Watch multiple paths using FSEvents.
   Returns a monitor object."
  [paths handler error-fn]
  (try
    (let [;; Track files that exist when we start watching
          known-files (atom (set (for [path paths
                                       file (file-seq (io/file path))
                                       :when (.isFile file)]
                                   (.getAbsolutePath file))))
          ;; Track when we started watching to filter historical events
          start-time (System/currentTimeMillis)
          ;; Track if we've been stopped
          stopped? (atom false)
          ;; Wrap handler to check stopped flag
          wrapped-handler (fn [event]
                            (when-not @stopped?
                              (try
                                ;; Only process events if they're not from the past
                                ;; FSEvents sometimes reports historical events on startup
                                (when-let [normalized (normalize-event event known-files)]
                                  ;; Give a small grace period (100ms) for startup
                                  (when (> (:timestamp normalized) (- start-time 100))
                                    (handler normalized)))
                                (catch Exception e
                                  (error-fn e {:event event})))))
          ;; Create the FSEvents monitor
          fsevents-monitor (fs/watch-async paths wrapped-handler)]
      ;; Return a monitor that includes the stopped flag
      (assoc fsevents-monitor :stopped? stopped?))
    (catch Exception e
      (error-fn e {:phase :start-fsevents})
      (throw e))))

(defn stop-all
  "Stop the FSEvents monitor."
  [monitor]
  (try
    ;; First set the stopped flag to prevent new events from being processed
    (when-let [stopped? (:stopped? monitor)]
      (reset! stopped? true))
    ;; Then stop the underlying FSEvents
    (fs/stop monitor)
    (catch Exception e
      ;; Log but don't throw - best effort
      (println "Error stopping FSEvents monitor:" e))))
