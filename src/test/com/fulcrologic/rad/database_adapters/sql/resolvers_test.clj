(ns com.fulcrologic.rad.database-adapters.sql.resolvers-test
  (:require
    [com.fulcrologic.rad.attributes :as rad.attr]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.resolvers :as sql.resolvers]
    [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
    [fulcro-spec.core :refer [specification component assertions when-mocking]]))

(declare =>)

(specification "delta->txs"
  (let [key->attribute {:account/id      attrs/account-id
                        :account/name    attrs/account-name
                        :account/locked? attrs/account-locked?
                        :user/id         attrs/user-id
                        :user/name       attrs/user-name}]
    (let [account-id         #uuid "ffffffff-ffff-ffff-ffff-000000000001"
          user-id            #uuid "ffffffff-ffff-ffff-ffff-000000000002"

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
                                                                :after  "Bob"}})]

      (component "Single level delta"
        (assertions
          "returns a single transaction"
          (count (sql.resolvers/delta->txs key->attribute account-delta)) => 1

          "skips attributes that don't need persisting"
          (sql.resolvers/delta->txs key->attribute account-delta-nosql) => []

          "skips values that didn't change"
          (sql.resolvers/delta->txs key->attribute account-delta-unchanged) => []))

      (component "Multi top level delta"
        (let [txs (sql.resolvers/delta->txs key->attribute multi-idents-delta)]
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
