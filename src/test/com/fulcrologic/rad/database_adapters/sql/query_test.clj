(ns com.fulcrologic.rad.database-adapters.sql.query-test
  (:require
    [com.fulcrologic.rad                                           :as rad]
    [com.fulcrologic.rad.database-adapters.sql.query               :as sql.query]
    [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
    [fulcro-spec.core :refer [specification component assertions]]
    [clojure.string :as str]))


(def simple-query
  [:account/id :account/name :account/active?])


(def nested-query
  [:account/id :account/name :account/active?
   {:account/addresses [:address/id
                        :address/street
                        :address/city]}])


(specification "query->plan"
  (let [params {::sql.query/id-attribute attrs/account-id
                ::rad/attributes   attrs/all-attributes}]

    (component "Top level query"
      (let [result (sql.query/query->plan simple-query params)]
        (assertions
          "selects the requested fields"
          (::sql.query/fields result)
          => [["accounts" ["id" "name" "active"]]]

          "uses the requested table"
          (::sql.query/from result)
          => "accounts")))

    (component "Nested query"
      (let [result (sql.query/query->plan nested-query params)]
        (assertions
          "Selects nested fields"
          (second (::sql.query/fields result))
          => ["addresses" ["id" "street" "city"]]

          "Joins to the nested table"
          (first (::sql.query/joins result))
          => [["addresses" "account_id"] ["accounts" "id"]])))))

(def EXPECTED_SQL ""
  (str/replace
    "
SELECT
 accounts.\"id\" AS accounts_id,
 accounts.\"name\" AS accounts_name,
 accounts.\"active\" AS accounts_active,
 addresses.\"id\" AS addresses_id,
 addresses.\"street\" AS addresses_street,
 addresses.\"city\" AS addresses_city
 FROM accounts
 LEFT JOIN addresses ON addresses.\"account_id\" = accounts.\"id\"
" #"\n" ""))


(specification "plan>sql"
  (let [params {::sql.query/id-attribute attrs/account-id
                ::rad/attributes   attrs/all-attributes}
        plan (sql.query/query->plan nested-query params)]

    (assertions "can interpret a plan into sql"
      (sql.query/plan->sql plan) => EXPECTED_SQL)))
