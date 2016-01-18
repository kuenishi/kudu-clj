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

(defn- make-column
  "Creates a ColumnSchemaBuilder object"
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
  "Creates a schema with Java-ish form (Schema Object)"
  [cols]
  (let [c (map make-column cols)]
    (Schema. (java.util.ArrayList. (into [] c)))))

(defn scan-all-data
  "Utility function. It just retrieve all rows in a table.
  Will be depcrecated."
  [client table_name]
  (let [scanner (.build (.newScannerBuilder client
                                            (.openTable client table_name)))
        it (fn [acc cont]
             (if (.hasMoreRows scanner)
               (cont (concat (iterator-seq (.iterator (.nextRows scanner))) acc) cont)
               acc))]
    (it () it)))

(defn connect!
  "Connect to Kudu server with a destination IP:Port argument"
  [dest]
  (.build (KuduClient$KuduClientBuilder. dest)))

(defn close!
  "Closes all related resources connected to a cluster"
  [conn]
  (.close conn))

(defn list-tables
  "List all tables resident in a cluster"
  [conn]
  (into [] (.getTablesList (.getTablesList conn))))

(defn table-exists
  "Client -> String -> Boolean
  Returns whether the tables exists"
  [conn table-name]
  (.tableExists conn table-name))

(defn open-table
  "Client -> String -> TableObject. Opens a table and make it ready to
  hit. Wrapper of KuduClient#openTable."
  [conn table-name]
  (.openTable conn table-name))

(defn create-table
  "Client -> String -> Schema
  Creates a table; Wrapper of KuduClient#createTable."
  [conn table-name schema]
  (let [native-schema (create-schema schema)]
    (.createTable conn table-name native-schema)))

(defn delete-table
  "Client -> String -> nil
  Wrapper of KuduClient#deleteTable."
  [conn table-name]
  (.deleteTable conn table-name))

(defn new-session
  "Client -> KuduSession, wrapper of KuduClient#newSession"
  [conn]
  (.newSession conn))

(defn- add-col
  "Add column info to PartialRow object."
  [partial-row col]
  (let [col-name (col :col)
        col-value (col :value)
        col-type (col :type)]
    (cond (= col-type :string)
          (.addString partial-row col-name col-value)
          (= col-type :binary)
          (.addBytes partial-row col-name col-value)
          (= col-type :int)
          (.addLong partial-row col-name col-value))))

(defn- handle-apply-response
  ""
  [operation-response]
  (if (.hasRowError operation-response)
    (.getRowError operation-response)
    [(.getWriteTimeStamp operation-response)
     (.getElapsedMillis operation-response)]))

(defn- build-mutation
  " mut is like
  {:type :insert,
   :rows [[{}]]
  }
  "
  [session table mut]
  (map
   (fn [row] ;; cols
     (let [mutator
           (cond (= (mut :type) :insert) (.newInsert table)
                 (= (mut :type) :update) (.newUpdate table)
                 (= (mut :type) :delete) (.newDelete table))
           partial-row
           (.getRow mutator)]
       (do
         (doseq [col row]
           (add-col partial-row col))
         (handle-apply-response
          (.apply session mutator)))))
   (mut :rows)))

(defn apply-mutation
  "
  mutation examples:
     [{:type :insert, :row [[{ :pk \"primary key\", ... }, ..]..]}
      {:type :delete, :row [[{ ... }}]}
      {:type :update, :row [[{ ... }}]}]
  "
  [session table mutations]
  (if (empty? mutations)
    ()
    (doseq [mutation mutations]
      (build-mutation session table mutation))))

(defn flush
  ""
  [session]
  (.flush session))

(defn scan
  ""
  [conn table-name preds proj aggr])

(defn list-tablet-servers
  ""
  [conn]
  (into [] (.getTabletServersList
            (.listTabletServers conn))))

(defn tablet-locations
  "Public: retri"
  [conn table-name]
  ;; TODO Translate LocatedTablet and LocatedTablet.Replica
  (into [] (.getTabletsLocations
            (.openTable conn table-name)
            10)))

;; TODO API around alter table
