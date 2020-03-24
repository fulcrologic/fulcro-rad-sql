(ns com.fulcrologic.rad.database-adapters.sql.query-test
  (:require
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.sql :as rsql]
    [com.fulcrologic.rad.database-adapters.sql.query :as query]
    [com.fulcrologic.rad.database-adapters.sql.migration :as mig]
    [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
    [clojure.string :as str]
    [taoensso.encore :as enc]
    [fulcro-spec.core :refer [specification component assertions]]
    [com.fulcrologic.rad.ids :as ids]
    [next.jdbc :as jdbc]
    [next.jdbc.sql :as sql]
    [taoensso.timbre :as log]))

(declare =>)
(def key->attribute (enc/keys-by ::attr/qualified-key attrs/all-attributes))

(specification "base-property-query" :focus
  (let [[actual attrs] (query/base-property-query {::attr/key->attribute key->attribute}
                         attrs/account-id
                         [attrs/account-email attrs/account-primary-address attrs/account-addresses]
                         [1 2 3])]
    (assertions
      "Generates the correct SQL"
      actual => "SELECT accounts.id AS c0,accounts.email AS c1,accounts.primary_address AS c2 FROM accounts WHERE id IN (1,2,3)"
      "returns the attributes that are in the query, in order"
      attrs => [attrs/account-id attrs/account-email attrs/account-primary-address])))

(specification "to-many-join-column-query"
  (let [[actual attrs] (query/to-many-join-column-query {::attr/key->attribute key->attribute}
                         attrs/account-addresses
                         [1 2 3])]
    (assertions
      "Generates the correct SQL"
      actual => "SELECT accounts.id AS c0, array_agg(addresses.id) AS c1 FROM accounts LEFT JOIN addresses ON accounts.id = addresses.accounts_addresses_accounts_id WHERE accounts.id IN (1,2,3) GROUP BY accounts.id"
      "Returns the attributes in the order they appear in the query"
      attrs => [attrs/account-id attrs/account-addresses])))

(specification "sql-results->edn-results"
  (let [id1 (ids/new-uuid 1)
        id2 (ids/new-uuid 2)]
    (assertions
      "can convert a sequence of SQL result maps on scalars to EDN result maps"
      (query/sql-results->edn-results [{:c0 id1 :c1 "joe"}
                                       {:c0 id2 :c1 "sam"}] [attrs/account-id attrs/account-name])
      => [{:account/id id1 :account/name "joe"}
          {:account/id id2 :account/name "sam"}]
      "Supports forward-ref to-one joins"
      (query/sql-results->edn-results [{:c0 id1 :c1 id1}
                                       {:c0 id2 :c1 id2}] [attrs/account-id attrs/account-primary-address])
      => [{:account/id id1 :account/primary-address {:address/id id1}}
          {:account/id id2 :account/primary-address {:address/id id2}}]
      "Supports aggregated result joins"
      (query/sql-results->edn-results [{:c0 id1 :c1 (object-array [id1 id2])}
                                       {:c0 id2 :c1 (object-array [id2])}]
        [attrs/account-id attrs/account-addresses])
      => [{:account/id id1 :account/addresses [{:address/id id1} {:address/id id2}]}
          {:account/id id2 :account/addresses [{:address/id id2}]}])))

(specification "eql->attrs"
  (assertions
    "Finds attributes, in order, that are in the given schema."
    (query/eql->attrs
      {::attr/key->attribute key->attribute}
      :production
      [:account/id :person/name {:account/primary-address [:address/id]}
       {:account/addresses [:address/id]}])
    =>
    [attrs/account-id attrs/account-primary-address attrs/account-addresses]))

(specification "eql-query!" :focus
  (let [id1 (ids/new-uuid 1)
        id2 (ids/new-uuid 2)
        id3 (ids/new-uuid 3)
        id4 (ids/new-uuid 4)
        ds  (jdbc/get-datasource {:dbtype "h2:mem"})]
    (with-open [c (.getConnection ds)]
      (doseq [s (mig/automatic-schema :production attrs/all-attributes)]
        (log/info s)
        (jdbc/execute! c [s]))
      (sql/insert! c "accounts" {:id id2 :name "sam"})
      (sql/insert! c "addresses" {:id id3 :street "111 Main St." :accounts_addresses_accounts_id id2})
      (sql/insert! c "addresses" {:id id4 :street "99 Main St." :accounts_addresses_accounts_id id2})
      (sql/insert! c "accounts" {:id id1 :name "joe" :primary_address id3})

      (assertions
        "Can resolve a non-batch ID-style resolver query"
        (query/eql-query! {::attr/key->attribute   key->attribute
                           ::rsql/connection-pools {:production c}}
          attrs/account-id
          [:account/name {:account/addresses [:address/street]}]
          {:account/id id1})
        => {:account/id id1 :account/name "joe" :account/addresses []}
        "Can resolve a batch query"
        (query/eql-query! {::attr/key->attribute   key->attribute
                           ::rsql/connection-pools {:production c}}
          attrs/account-id
          [:account/name {:account/addresses [:address/street]}]
          [{:account/id id1}])
        => [{:account/id id1 :account/name "joe" :account/addresses []}]
        "Can resolve to-one forward refs"
        (query/eql-query! {::attr/key->attribute   key->attribute
                           ::rsql/connection-pools {:production c}}
          attrs/account-id
          [:account/name {:account/primary-address [:address/street]}]
          {:account/id id1})
        => {:account/id id1 :account/name "joe" :account/primary-address {:address/id id3}}
        "Can resolve to-many reverse refs"
        (query/eql-query! {::attr/key->attribute   key->attribute
                           ::rsql/connection-pools {:production c}}
          attrs/account-id
          [{:account/addresses [:address/street]}]
          {:account/id id2})
        => {:account/id id2 :account/addresses [{:address/id id3}
                                                {:address/id id4}]}))))
