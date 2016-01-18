(ns kududb.client-test
  (:require [clojure.test :refer :all]
            [kududb.client :refer :all]
            [kududb.ql :refer :all]))

;; (:import [org.kududb.client])

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))

(defn maybe-put-some-data
  ""
  [client table-name]
  (let [session (new-session client)
        mutations [{:type :insert
                    :rows [[{:col "pk" :type :string :value "ppkk"}
                            {:col "val" :type :string :value "asfd"}]]}]
        table (.openTable client table-name)]

    (do
      (println "apply>" (apply-mutation session table mutations))
      (println "flush>" (flush session)))))

(deftest main-test
  (testing "Connecting to KUDU service"

    (let [c (connect! "127.0.0.1:7051")
          schema [{:name "pk", :type :string, :primary true}
                  {:name "val", :type :string}]
          table_name "test-table"]
      (do
        (println "before table creation:" (list-tables c))

        (if (not (table-exists c table_name))
          (create-table c table_name schema)
          (println "table" table_name "already exists."))

        (println "list table servers:" (list-tablet-servers c))

        (println "after table creation:" (list-tables c))

        (println "scanning all data:")
        (println (map (fn [rr] (.rowToString rr))
                      (scan-all-data c table_name)))

        (println "putting some data:")
        (println (maybe-put-some-data c table_name))

        (println "scanning all data:")
        ;; why this doesn't print out anything????
        (println (map (fn [rr] (.rowToString rr))
                      (scan-all-data c table_name)))

        (println "Get tablet locations:"
                 (tablet-locations c table_name))

        (close! c)))))

(deftest ql-test
  (testing "ql test"
    (println
     (translate-tree
      '(:select ["column-name"]
                :from [(:table "table-name" "t")]
                :where (eq :col "cola" 10))
      ))))
