(defproject metabase/elastic6-driver "1.0.0-SNAPSHOT"
  :min-lein-version "2.5.0"

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [org.clojure/data.json "0.2.7"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "elastic6.metabase-driver.jar"}})
