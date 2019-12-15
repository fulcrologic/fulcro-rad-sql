(ns com.fulcrologic.rad.database-adapters.sql-test
  (:require
    [com.fulcrologic.rad.attributes            :as rad.attr :refer [defattr]]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [clojure.test :refer [deftest testing is]]))

(defattr id :account/id :uuid
  {::rad.attr/identity? true
   ::rad.sql/schema :production
   ::rad.sql/tables #{"accounts"}})

(defattr name :account/name :string
  {::rad.sql/schema         :production
   ::rad.sql/tables         #{"accounts"}})

(deftest save-form-steps-test
  (with-redefs [rad.attr/key->attribute {:account/id id
                                         :account/name name}]
    (testing "It returns a list of updates"
      (let [id #uuid "ffffffff-ffff-ffff-ffff-000000000001"
            delta {[:account/id id] #:account{:name {:before "Bob"
                                                     :after "Alice"}}}]
        (is (= [{::rad.sql/table  "accounts"
                 ::rad.sql/schema :production
                 :tx/action       :sql/update
                 :tx/attrs        {:account/name "Alice"}
                 :tx/where        {:account/id id}}]
              (rad.sql/save-form-steps delta)))))))
