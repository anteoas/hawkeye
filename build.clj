(ns build
  (:require [clojure.tools.build.api :as b]))

(def class-dir "classes")
(def basis (b/create-basis {:project "deps.edn"}))

(defn compile-java
  "Compile Java sources for FSEvents support on macOS.
   This is called automatically via :deps/prep-lib when the library is used as a git dependency."
  [_]
  (println "Compiling Java sources for hawkeye...")
  (b/javac {:src-dirs ["src"]
            :class-dir class-dir
            :basis basis
            :javac-opts ["--release" "11"]})
  (println "Java compilation complete."))