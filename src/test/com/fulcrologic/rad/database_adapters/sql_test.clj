(ns com.fulcrologic.rad.database-adapters.sql-test
  (:require
    [com.fulcrologic.rad.attributes            :as rad.attr :refer [defattr]]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [fulcro-spec.core :refer [specification component when-mocking assertions]]))

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

(specification "delta->txs"
  (when-mocking
    (rad.attr/key->attribute k) => (get {:account/id      account-id
                                         :account/name    account-name
                                         :account/locked? account-locked?
                                         :user/id         user-id
                                         :user/name       user-name}
                                     k)

    (let [account-id #uuid "ffffffff-ffff-ffff-ffff-000000000001"
          user-id    #uuid "ffffffff-ffff-ffff-ffff-000000000002"

          account-delta
          {[:account/id account-id] #:account{:name {:before "Joe's pancakes"
                                                     :after  "Jeff's waffles"}}}
          account-delta-nosql
          {[:account/id account-id] #:account{:locked? {:before false
                                                        :after  true}}}
          account-delta-unchanged
          {[:account/id account-id] #:account{:name {:before "Jim"
                                                     :after  "Jim"}}}
          multi-idents-delta (assoc account-delta
                               [:user/id user-id] #:user{:name {:before "Alice"
                                                                :after "Bob"}})]

      (component "Single level delta"
        (assertions
          "returns a single transaction"
          (count (rad.sql/delta->txs account-delta)) => 1

          "skips attributes that don't need persisting"
          (rad.sql/delta->txs account-delta-nosql) => []

          "skips values that didn't change"
          (rad.sql/delta->txs account-delta-unchanged) => []))

      (component "Multi top level delta"
        (let [txs (rad.sql/delta->txs multi-idents-delta)]
          (assertions
            "returns multiple statements"
            (count txs) => 2

            "targets the correct tables"
            (map ::rad.sql/table txs) => ["accounts" "users"]

            "targets the correct entities"
            (map :tx/where txs) => [{:account/id account-id} {:user/id user-id}]

            "describes the new attributes"
            (map :tx/attrs txs) => [{:account/name "Jeff's waffles"}
                                    {:user/name "Bob"}]))))))
