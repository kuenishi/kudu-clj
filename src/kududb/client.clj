(ns kududb.client
  "Netowrk client for connecting to a Kudu cluster"
  (:import (org.kududb.client KuduClient
                              KuduClient$KuduClientBuilder)
           (org.kududb ColumnSchema
                       ColumnSchema$ColumnSchemaBuilder
                       Schema
                       Type)))


(def to-type
  {:string Type/STRING,
   :binary Type/BINARY,
   :double Type/DOUBLE,
   :int32 Type/INT32})

(defn make-column
  "Creates a column"
  [col]
  (.build
   ;; doto must keep the same type at each stage of pipe
   (doto (ColumnSchema$ColumnSchemaBuilder.
          (col :name)
          (to-type (col :type)))
     (.key (some? (col :primary)))
     (.nullable (some? (col :nullable)))
     )))

(defn create-schema
  "Creates a schema with Java-ish form"
  [cols]
  (let [c (map make-column cols)]
    (Schema. (java.util.ArrayList. (into [] c)))))

(defn scan-all-data
  ""
  [client table_name]
  (let [scanner (.build (.newScannerBuilder client
                                            (.openTable client table_name)))
        it (fn [acc cont]
             (if (.hasMoreRows scanner)
               (cont (concat (iterator-seq (.iterator (.nextRows scanner))) acc) cont)
               acc))]
    (it () it)))

(defn maybe-put-some-data
  ""
  [client table_name]
  (let [session (.newSession client)
        table (.openTable client table_name)]
    (let [insert (.newInsert table)]
    ;;(let [insert (.newUpdate table)]
      (do
        (doto (.getRow insert)
          (.addString "pk" "adafds")
          (.addString "val" "super dooper blooper2"))
        (println (.apply session insert)
                 (.flush session))))))


(defn connect!
  ""
  [dest]
  (.build (KuduClient$KuduClientBuilder. dest)))

(defn close!
  ""
  [conn]
  (.close conn))

(defn list-tables
  ""
  [conn]
  (into [] (.getTablesList (.getTablesList conn))))

(defn table-exists
  ""
  [conn table-name]
  (.tableExists conn table-name))

(defn open-table
  ""
  [conn table-name]
  (.openTable conn table-name))

(defn create-table
  ""
  [conn table-name schema]
  (let [native-schema (create-schema schema)]
    (.createTable conn table-name native-schema)))

(defn delete-table
  ""
  [conn table-name]
  (.deleteTable conn table-name))

(defn new-session
  ""
  [conn]
  (.newSession conn))

(defn apply-mutation
  ""
  [conn session])

(defn flush
  ""
  [session])

(defn scan
  ""
  [conn table-name preds proj aggr])

(defn list-tablet-servers
  ""
  [conn]
  (into [] (.getTabletServersList
            (.listTabletServers conn))))

(defn tablet-locations
  ""
  [conn table-name]
  ;; TODO Translate LocatedTablet and LocatedTablet.Replica
  (into [] (.getTabletsLocations
            (.openTable conn table-name)
            10)))

;; TODO API around alter table
