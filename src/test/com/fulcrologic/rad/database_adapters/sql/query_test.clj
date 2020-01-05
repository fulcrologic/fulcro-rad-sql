(ns com.fulcrologic.rad.database-adapters.sql.query-test
  (:require
    [com.fulcrologic.rad                                           :as rad]
    [com.fulcrologic.rad.database-adapters.sql.query               :as sql.query]
    [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
    [fulcro-spec.core :refer [specification component assertions]]
    [clojure.string :as str]))


(def SIMPLE_QUERY
  [:account/id :account/name :account/active?])


(def NESTED_QUERY
  [:account/id :account/name :account/active?
   {:account/addresses [:address/id
                        :address/street
                        :address/city]}])


(def NESTED_QUERY_NO_PK
  [:account/name {:account/addresses [:address/street]}])


(specification "query->plan"
  (let [params {::sql.query/id-attribute attrs/account-id
                ::rad/attributes   attrs/all-attributes}]

    (component "Top level query"
      (let [result (sql.query/query->plan SIMPLE_QUERY params)]
        (assertions
          "selects the requested fields"
          (::sql.query/fields result)
          => [["accounts" ["id" "name" "active"]]]

          "uses the requested table"
          (::sql.query/from result)
          => "accounts")))

    (component "Nested query"
      (let [result (sql.query/query->plan NESTED_QUERY params)]
        (assertions
          "Selects nested fields"
          (second (::sql.query/fields result))
          => ["addresses" ["id" "street" "city"]]

          "Joins to the nested table"
          (first (::sql.query/joins result))
          => [["addresses" "account_id"] ["accounts" "id"]])))

    (component "Nested query without primary keys"
      (let [result (sql.query/query->plan NESTED_QUERY_NO_PK params)]
        (assertions
          "selects top-level primary keys anyway"
          (first (::sql.query/fields result))
          => ["accounts" ["id" "name"]]

          "selects nested primary keys anyway"
          (second (::sql.query/fields result))
          => ["addresses" ["id" "street"]])))))


(def EXPECTED_SQL ""
  (str/replace
    "
SELECT
 accounts.\"id\",
 accounts.\"name\",
 accounts.\"active\",
 addresses.\"id\",
 addresses.\"street\",
 addresses.\"city\"
 FROM accounts
 LEFT JOIN addresses ON addresses.\"account_id\" = accounts.\"id\"
" #"\n" ""))


(specification "plan>sql"
  (let [params     {::sql.query/id-attribute attrs/account-id
                    ::rad/attributes   attrs/all-attributes}
        plan       (sql.query/query->plan NESTED_QUERY params)]

    (assertions "interprets a plan with joins"
      (sql.query/plan->sql plan) => EXPECTED_SQL)))
