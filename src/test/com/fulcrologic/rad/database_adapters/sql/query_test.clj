(ns com.fulcrologic.rad.database-adapters.sql.query-test
  (:require
    [com.fulcrologic.rad.attributes                                :as rad.attr]
    [com.fulcrologic.rad.database-adapters.sql                     :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.query               :as sql.query]
    [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
    [clojure.string                                                :as str]
    [taoensso.encore                                               :as enc]
    [fulcro-spec.core :refer [specification component assertions]]))


(def SIMPLE_QUERY
  [:account/id :account/name :account/active?])


(def NESTED_QUERY
  [:account/id :account/name :account/active?
   {:account/addresses [:address/id
                        :address/street
                        :address/city]}])


(specification "query->plan"
  (let [params {::rad.sql/id-attribute attrs/account-id
                ::rad.attr/k->attr     (enc/keys-by ::rad.attr/qualified-key
                                         attrs/all-attributes)}
        simplify (juxt ::rad.attr/qualified-key ::rad.sql/table ::rad.sql/column)]

    (component "Top level query"
      (let [result   (sql.query/query->plan SIMPLE_QUERY {} params)]
        (assertions
          "selects the requested fields"
          (map simplify (::sql.query/fields result))
          => [[:account/id      "accounts" "id"]
              [:account/name    "accounts" "name"]
              [:account/active? "accounts" "active"]]

          "uses the requested table"
          (::sql.query/from result)
          => "accounts"

          "doesn't need grouping"
          (::sql.query/group result)
          => nil)))

    (component "Nested query"
      (let [result (sql.query/query->plan NESTED_QUERY {} params)
            joined-fields (drop 3 (::sql.query/fields result))]
        (assertions
          "Selects nested fields"
          (map simplify joined-fields)
          => [[:address/id     "addresses" "id"]
              [:address/street "addresses" "street"]
              [:address/city   "addresses" "city"]]

          "Keeps the parent-key"
          (map ::sql.query/parent-key joined-fields)
          => [:account/addresses :account/addresses :account/addresses]

          "Uses cardinality of parent"
          (map ::rad.attr/cardinality joined-fields)
          => [:many :many :many]

          "Joins to the nested table"
          (first (::sql.query/joins result))
          => [["addresses" "account_id"] ["accounts" "id"]]

          "Groups by parent's PK"
          (::sql.query/group result)
          => [["accounts" "id"]])))))


(def EXPECTED_SQL ""
  (str/replace
    "
SELECT
 accounts.\"id\",
 accounts.\"name\",
 accounts.\"active\",
 array_agg(addresses.\"id\"),
 array_agg(addresses.\"street\"),
 array_agg(addresses.\"city\")
 FROM accounts
 LEFT JOIN addresses ON addresses.\"account_id\" = accounts.\"id\"
  GROUP BY accounts.\"id\"
" #"\n" ""))


(def EXPECTED_SQL_WHERE ""
  (str/replace
    "
SELECT
 accounts.\"id\",
 accounts.\"name\",
 accounts.\"active\",
 array_agg(addresses.\"id\"),
 array_agg(addresses.\"street\"),
 array_agg(addresses.\"city\")
 FROM accounts
 LEFT JOIN addresses ON addresses.\"account_id\" = accounts.\"id\"
 WHERE accounts.\"id\" = ?
 GROUP BY accounts.\"id\"
" #"\n" ""))


(specification "plan>sql"
  (let [params     {::rad.sql/id-attribute attrs/account-id
                    ::rad.attr/k->attr     (enc/keys-by ::rad.attr/qualified-key
                                             attrs/all-attributes)}
        plan       (sql.query/query->plan NESTED_QUERY {} params)
        plan-where (sql.query/query->plan NESTED_QUERY {:account/id 12} params)]

    (assertions "interprets a plan with joins"
      (sql.query/plan->sql plan) => [EXPECTED_SQL])

    (assertions "interprets a plan with a where clause"
      (sql.query/plan->sql plan-where) => [EXPECTED_SQL_WHERE 12])))
