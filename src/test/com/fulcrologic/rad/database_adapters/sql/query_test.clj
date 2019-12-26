(ns com.fulcrologic.rad.database-adapters.sql.query-test
  (:require
    [com.fulcrologic.rad                                           :as rad]
    [com.fulcrologic.rad.database-adapters.sql.query               :as sql.query]
    [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
    [fulcro-spec.core :refer [specification component assertions]]))


(specification "query->plan"
  (let [simple-query [:account/id :account/name :account/active?]
        nested-query [:account/id :account/name :account/active?
                      {:account/addresses [:address/id
                                           :address/street
                                           :address/city]}]
        params {::sql.query/id-attribute attrs/account-id
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
