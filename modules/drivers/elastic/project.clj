(defproject metabase/elastic-driver "1.0.0-SNAPSHOT"
  :min-lein-version "2.5.0"

  :dependencies
  [[org.clojure/data.json "0.2.7"]
   [clojure.java-time "0.3.2"]]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    ; :omit-source    true
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "elastic.metabase-driver.jar"}})
