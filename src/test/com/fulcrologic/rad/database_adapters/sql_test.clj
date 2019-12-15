(ns com.fulcrologic.rad.database-adapters.sql-test
  (:require
    [com.fulcrologic.rad.attributes            :as rad.attr :refer [defattr]]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [clojure.test :refer [deftest testing is]]))

(defattr account-id :account/id :uuid
  {::rad.attr/identity? true
   ::rad.sql/schema :production
   ::rad.sql/tables #{"accounts"}})

(defattr account-name :account/name :string
  {::rad.sql/schema :production
   ::rad.sql/tables #{"accounts"}})

;; Derived data
(defattr account-locked? :account/locked? :boolean
  {})

(defattr user-id :user/id :uuid
  {::rad.sql/schema :production
   ::rad.sql/tables #{"users"}})

(defattr user-name :user/name :string
  {::rad.sql/schema :production
   ::rad.sql/tables #{"users"}})

(deftest delta->txs-test
  (with-redefs [rad.attr/key->attribute {:account/id      account-id
                                         :account/name    account-name
                                         :account/locked? account-locked?
                                         :user/id         user-id
                                         :user/name       user-name}]

    (let [account-id #uuid "ffffffff-ffff-ffff-ffff-000000000001"
          user-id    #uuid "ffffffff-ffff-ffff-ffff-000000000002"

          account-delta
          {[:account/id account-id] #:account{:name {:before "Joe's pancakes"
                                                     :after  "Jeff's waffles"}}}
          account-delta-nosql
          {[:account/id account-id] #:account{:locked? {:before false
                                                        :after  true}}}
          multi-idents-delta
          {[:account/id account-id] #:account{:name {:before "Joe's pancakes"
                                                     :after "Jeff's waffles"}}
           [:user/id user-id]       #:user{:name {:before "Alice"
                                                  :after "Bob"}}}

          accounts-tx {::rad.sql/table  "accounts"
                       ::rad.sql/schema :production
                       :tx/action       :sql/update
                       :tx/attrs        {:account/name "Jeff's waffles"}
                       :tx/where        {:account/id account-id}}

          users-tx     {::rad.sql/table  "users"
                        ::rad.sql/schema :production
                        :tx/action       :sql/update
                        :tx/attrs        {:user/name "Bob"}
                        :tx/where        {:user/id user-id}}]

      (testing "It returns a list of updates"
        (is (= [accounts-tx] (rad.sql/delta->txs account-delta))))

      (testing "It won't try to persist non-sql attributes"
        (is (empty? (rad.sql/delta->txs account-delta-nosql))))

      (testing "It will use mutliple statements for "
        (is (= [accounts-tx users-tx]
              (rad.sql/delta->txs multi-idents-delta)))))))
