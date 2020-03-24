(ns com.fulcrologic.rad.database-adapters.sql.resolvers-test
  (:require
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.sql :as rsql]
    [com.fulcrologic.rad.database-adapters.sql.resolvers :as res]
    [com.fulcrologic.rad.database-adapters.test-helpers.attributes :as attrs]
    [fulcro-spec.core :refer [specification component assertions when-mocking]]
    [taoensso.encore :as enc]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]))

(def key->attribute (enc/keys-by ::attr/qualified-key attrs/all-attributes))
(declare =>)

(specification "scalar-insert"
  (let [tempid (tempid/tempid)]
    (assertions
      "will only insert updates to non-nil scalar fields"
      (res/scalar-insert {::attr/key->attribute key->attribute} :production {tempid 1} [:account/id tempid]
        {:account/name            {:before "joe" :after "sam"}
         :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                   :after  [[:address/id 22]]}
         :account/active?         {:before true :after false}
         :account/primary-address {:before 3 :after 4}})
      => ["INSERT INTO accounts (id,name,active) VALUES (?,?,?)" 1 "sam" false]
      "ignores rows that are not new"
      (nil?
        (res/scalar-insert {::attr/key->attribute key->attribute} :production {} [:account/id 1]
          {:account/name            {:before "joe" :after "sam"}
           :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                     :after  [[:address/id 22]]}
           :account/active?         {:before true :after false}
           :account/primary-address {:before 3 :after 4}}))
      => true
      "ignores attrs that are on a difference schema"
      (nil?
        (res/scalar-insert {::attr/key->attribute key->attribute} :other {tempid 1} [:account/id tempid]
          {:account/name            {:before "joe" :after "sam"}
           :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                     :after  [[:address/id 22]]}
           :account/active?         {:before true :after false}
           :account/primary-address {:before 3 :after 4}}))
      => true)))

(specification "scalar-update"
  (let [tempid (tempid/tempid)]
    (assertions
      "ignores new rows"
      (nil?
        (res/scalar-update {::attr/key->attribute key->attribute} :production [:account/id tempid]
          {:account/name            {:before "joe" :after "sam"}
           :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                     :after  [[:address/id 22]]}
           :account/active?         {:before true :after false}
           :account/primary-address {:before 3 :after 4}}))
      => true
      "updates changed scalars to correct new values"
      (res/scalar-update {::attr/key->attribute key->attribute} :production [:account/id 1]
        {:account/name            {:before "joe" :after "sam"}
         :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                   :after  [[:address/id 22]]}
         :account/active?         {:before true :after false}
         :account/primary-address {:before 3 :after 4}})
      => ["UPDATE accounts SET name = ?,active = ? WHERE id = ?"
          "sam" false 1]
      "ignores changes for other schema"
      (nil?
        (res/scalar-update {::attr/key->attribute key->attribute} :other [:account/id 1]
          {:account/name            {:before "joe" :after "sam"}
           :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                     :after  [[:address/id 22]]}
           :account/active?         {:before true :after false}
           :account/primary-address {:before 3 :after 4}}))
      => true)))

(specification "to-one-ref-update"
  (let [tempid (tempid/tempid)]
    (assertions
      "ignores columns on other schemas"
      (nil? (res/to-one-ref-update
              {::attr/key->attribute key->attribute}
              :other
              attrs/account-id
              {tempid 42}
              tempid
              attrs/account-primary-address
              nil
              [:address/id 5]))
      => true
      "updates to-one column to correct new values"
      (res/to-one-ref-update
        {::attr/key->attribute key->attribute}
        :production
        attrs/account-id
        {tempid 42}
        tempid
        attrs/account-primary-address
        nil
        [:address/id 5])
      => ["UPDATE accounts SET primary_address = ? WHERE id = ?" 5 42]
      "Sets the column to NULL when removed"
      (res/to-one-ref-update
        {::attr/key->attribute key->attribute}
        :production
        attrs/account-id
        {}
        42
        attrs/account-primary-address
        [:address/id 5]
        nil)
      => ["UPDATE accounts SET primary_address = NULL WHERE id = ?" 42]
      "Deletes the referent row when removed if that option is set"
      (res/to-one-ref-update
        {::attr/key->attribute key->attribute}
        :production
        attrs/account-id
        {}
        42
        (assoc attrs/account-primary-address ::rsql/delete-referent? true)
        [:address/id 5]
        nil)
      => ["DELETE FROM addresses WHERE id = ?" 5])))

(specification "to-many-ref-update"
  (let [tempid (tempid/tempid)]
    (assertions
      "updates to-one column to correct new values"
      (res/to-many-ref-update
        {::attr/key->attribute key->attribute}
        :production
        attrs/account-id
        {tempid 42}
        tempid
        attrs/account-addresses
        nil
        [[:address/id 5]])
      => [["UPDATE addresses SET accounts_addresses_accounts_id = ? WHERE id = ?" 42 5]]
      "Sets the column to NULL when removed"
      (res/to-many-ref-update
        {::attr/key->attribute key->attribute}
        :production
        attrs/account-id
        {}
        42
        attrs/account-addresses
        [[:address/id 5]]
        nil)
      => [["UPDATE addresses SET accounts_addresses_accounts_id = NULL WHERE id = ?" 5]]
      "Returns a change for every add/remove"
      (res/to-many-ref-update
        {::attr/key->attribute key->attribute}
        :production
        attrs/account-id
        {tempid 42}
        tempid
        attrs/account-addresses
        [[:address/id 1] [:address/id 10]]
        [[:address/id 5] [:address/id 10]])
      => [["UPDATE addresses SET accounts_addresses_accounts_id = ? WHERE id = ?" 42 5]
          ["UPDATE addresses SET accounts_addresses_accounts_id = NULL WHERE id = ?" 1]]
      "Ignores to-many attributes from another schema"
      (nil?
        (res/to-many-ref-update
          {::attr/key->attribute key->attribute}
          :other
          attrs/account-id
          {tempid 42}
          tempid
          attrs/account-addresses
          [[:address/id 1] [:address/id 10]]
          [[:address/id 5] [:address/id 10]]))
      => true
      "Deletes the referent row when removed if that option is set"
      (res/to-many-ref-update
        {::attr/key->attribute key->attribute}
        :production
        attrs/account-id
        {}
        42
        (assoc attrs/account-addresses ::rsql/delete-referent? true)
        [[:address/id 5]]
        nil)
      => [["DELETE FROM addresses WHERE id = ?" 5]])))
