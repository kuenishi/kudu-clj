(defproject kudu-clj "0.0.1-SNAPSHOT"
  :description "Kudu client for Clojure"
  :url "https://github.com/kuenishi/kudu-clj"
  :license {:name "Apache License 2"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.kududb/kudu-client "0.6.0"]]
                  ;:exclusions [org.slf4j/slf4j-api]]]
  :repositories [["Cloudera repo"
                  "https://repository.cloudera.com/artifactory/repo/"]]
  :main ^:skip-aot kududb.client)
