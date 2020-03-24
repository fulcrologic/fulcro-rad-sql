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
      (res/scalar-insert {::attr/key->attribute key->attribute} {tempid 1} [:account/id tempid]
        {:account/name            {:before "joe" :after "sam"}
         :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                   :after  [[:address/id 22]]}
         :account/active?         {:before true :after false}
         :account/primary-address {:before 3 :after 4}})
      => "INSERT INTO accounts (id,name,active) VALUES (1,'sam',false)"
      "ignores rows that are not new"
      (nil?
        (res/scalar-insert {::attr/key->attribute key->attribute} {} [:account/id 1]
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
        (res/scalar-update {::attr/key->attribute key->attribute} [:account/id tempid]
          {:account/name            {:before "joe" :after "sam"}
           :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                     :after  [[:address/id 22]]}
           :account/active?         {:before true :after false}
           :account/primary-address {:before 3 :after 4}}))
      => true
      "updates changed scalars to correct new values"
      (res/scalar-update {::attr/key->attribute key->attribute} [:account/id 1]
        {:account/name            {:before "joe" :after "sam"}
         :account/addresses       {:before [[:address/id 5] [:address/id 6]]
                                   :after  [[:address/id 22]]}
         :account/active?         {:before true :after false}
         :account/primary-address {:before 3 :after 4}})
      => "UPDATE accounts SET name = 'sam',active = false WHERE id = 1")))

(specification "to-one-ref-update"
  (let [tempid (tempid/tempid)]
    (assertions
      "updates to-one column to correct new values"
      (res/to-one-ref-update
        {::attr/key->attribute key->attribute}
        attrs/account-id
        {tempid 42}
        tempid
        attrs/account-primary-address
        nil
        [:address/id 5])
      => "UPDATE accounts SET primary_address = 5 WHERE id = 42")))
