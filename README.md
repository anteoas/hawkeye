# hawkeye

A lightweight, cross-platform file watching library for Clojure with debouncing support. 

## Features

- 🚀 **Fast & Efficient**: Uses native FSEvents on macOS, virtual threads on Java 21+, and polling elsewhere
- 🎯 **Simple API**: Just two main functions - `watch` and `debounce`
- 🔄 **Auto-recursive**: Automatically watches subdirectories and newly created directories
- ⚡ **Debouncing Built-in**: Avoid triggering on rapid-fire changes
- 🛡️ **Robust Error Handling**: Graceful fallbacks and clear error reporting
- 📦 **Minimal Dependencies**: Core library has zero dependencies, FSEvents support requires JNA

## Installation

1. Add to your `deps.edn`:

```clojure
{:deps {io.github.anteoas/hawkeye {:git/sha "SHA"}}}
```

2. Run the prep step (required once after adding/updating the dependency):

```bash
clojure -X:deps prep
```

This compiles the Java sources needed for FSEvents support on macOS. You only need to do this once per project.

## Quick Start

```clojure
(require '[hawkeye.core :as hawk])

;; Start watching directories
(def stop (hawk/watch ["src" "resources"]
                      (fn [event]
                        (println (:type event) ":" (:file event)))
                      (fn [error context]
                        (println "Error:" error))))

;; When done, stop watching
(stop)
```

## API

### `watch`

Watch directories for file system changes.

```clojure
(watch paths notify-fn error-fn)
(watch paths notify-fn error-fn options)
```

**Arguments:**
- `paths` - Collection of directory paths to watch (always recursive), or a single string path
- `notify-fn` - Called with event map: `{:type :create/:modify/:delete, :file "name", :path "full/path", :timestamp ms}`
- `error-fn` - Called with exception and context map when errors occur
- `options` - Optional map with:
  - `:mode` - `:auto` (default), `:vthread`, `:poll`, or `:fsevents`
  - `:poll-ms` - Polling interval in milliseconds (default: 10, only used in `:poll` mode)

**Returns:** A zero-argument stop function with metadata containing the actual mode used

### `debounce`

Create a debounced version of a function that only executes after a quiet period.

```clojure
(debounce f delay-ms)
(debounce f delay-ms :events mode)
```

**Arguments:**
- `f` - Function to debounce
- `delay-ms` - Milliseconds to wait before calling
- `:events` - How to handle multiple calls:
  - `:last` (default) - Use only the last arguments
  - `:first` - Use only the first arguments
  - `:all` - Pass vector of all argument sets
  - `:unique` - Pass vector of unique argument sets

## Examples

### Basic File Watching

```clojure
;; Watch a single directory (string path is automatically wrapped in a vector)
(def stop (hawk/watch "src"
                      (fn [{:keys [type file]}]
                        (println type "-" file))
                      (fn [e _]
                        (println "Error:" e))))

;; Watch multiple directories
(def stop (hawk/watch ["src" "test" "resources"]
                      (fn [{:keys [type file path]}]
                        (case type
                          :create (println "Created:" file)
                          :modify (println "Modified:" file)
                          :delete (println "Deleted:" file)))
                      (fn [e ctx]
                        (println "Watch error:" (.getMessage e)))))

;; Check which mode was actually used
(println "Watch mode:" (:hawk-eye/mode (meta stop)))
```

### Development Auto-rebuild

```clojure
(defn rebuild! []
  (println "Rebuilding...")
  (compile-my-project))

;; Debounce to avoid multiple rapid rebuilds
(def debounced-rebuild (hawk/debounce rebuild! 200))

(def stop (hawk/watch ["src" "resources"]
                      (fn [_] (debounced-rebuild))
                      (fn [e _] (println "Error:" e))))
```

### Multiple Directories with Filtering

```clojure
(def stop (hawk/watch ["src" "test" "resources"]
                      (fn [{:keys [file] :as event}]
                        ;; Only react to Clojure files
                        (when (re-matches #".*\.clj[cs]?$" file)
                          (println "Clojure file changed:" file)
                          (run-tests)))
                      (fn [e _] (println "Error:" e))))
```

### Using Specific Watch Mode

```clojure
;; Force polling mode (useful for network drives)
(def stop (hawk/watch ["network-drive/shared"]
                      handler
                      error-handler
                      {:mode :poll
                       :poll-ms 100}))

;; Explicitly use FSEvents on macOS
(def stop (hawk/watch ["src"]
                      handler
                      error-handler
                      {:mode :fsevents}))
```

### Advanced Debouncing

```clojure
;; Collect all events that happen within 100ms
(def collect-events (hawk/debounce 
                      (fn [events]
                        (println "Got" (count events) "events")
                        (doseq [e events]
                          (println "  -" (:type e) (:file e))))
                      100
                      :events :all))

(def stop (hawk/watch ["src"] collect-events (fn [e _] (println "Error:" e))))
```

## Platform-Specific Behavior

### macOS (FSEvents)

On macOS, Java's WatchService has a ~2 second delay for detecting file changes. To avoid this, hawkeye uses the native FSEvents API which provides near-instant file event detection.

If FSEvents initialization fails, you'll see a warning and hawkeye will fall back to the slower WatchService polling:

```
WARNING: FSEvents initialization failed. Falling back to slower WatchService polling.
  Cause: <error message>
  Note: WatchService polling is significantly slower on macOS.
```

When this happens, file events will be delayed by approximately 2 seconds.

### Java 21+ (Virtual Threads)

On Java 21+, hawkeye automatically uses virtual threads for better resource efficiency when not on macOS.

### Other Platforms

Uses standard WatchService with efficient polling for maximum compatibility.

## Implementation Details

Hawkeye uses different strategies based on the platform and available features:

1. **FSEvents (macOS)** - Native macOS file system events, extremely efficient
2. **Virtual Threads (Java 21+)** - Uses virtual threads with blocking I/O for efficiency
3. **Polling** - Falls back to WatchService polling for compatibility

The `:auto` mode (default) automatically selects the best available strategy.

## Performance Tips

1. **Use Debouncing**: File systems can generate many events for a single logical change
2. **Filter Events**: Process only the files you care about in your notify-fn
3. **Handle Errors**: Always provide an error-fn to handle and log issues
4. **Stop Watchers**: Always call the stop function when done to free resources
5. **Enable FSEvents on macOS**: Add the JVM flag for significantly better performance

## Development

### Project Structure

```
hawkeye/
├── src/
│   └── hawkeye/
│       ├── core.clj           # Main API
│       └── fsevents/          # macOS FSEvents support
│           ├── core.clj       # JNA bindings
│           ├── monitor.clj    # Integration layer
│           └── FSEventCallback.java
├── test/
│   └── hawkeye/
│       └── core_test.clj
├── deps.edn
└── build.clj                  # Build configuration
```

### Running Tests

```bash
clojure -X:test
```

### Building

Compile Java sources:
```bash
clojure -T:build compile-java
```

Build JAR:
```bash
clojure -T:build jar
```

## Troubleshooting

### "WARNING: A restricted method in java.lang.System has been called"

This warning appears when using FSEvents on macOS without the proper JVM flag. Add `--enable-native-access=ALL-UNNAMED` to your JVM options. The library will still work but will use the slower polling method.

### Missing Events

Some file systems or editors create temporary files and rename them. You might see delete/create events instead of modify events. This is normal behavior.

### Events Not Detected

- Ensure the directories exist before watching
- Check that your error-fn is handling exceptions
- On some systems, very rapid changes might be coalesced
