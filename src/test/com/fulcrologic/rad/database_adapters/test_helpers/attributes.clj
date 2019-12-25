(ns com.fulcrologic.rad.database-adapters.test-helpers.attributes
  (:require
    [com.fulcrologic.rad.attributes            :as rad.attr :refer [defattr]]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Account

(defattr account-id :account/id :uuid
  {::rad.attr/identity? true
   ::rad.sql/schema :production
   ::rad.sql/tables #{"accounts"}})


(defattr account-name :account/name :string
  {::rad.sql/schema :production
   ::rad.sql/tables #{"accounts"}})


(defattr account-active? :account/active? :boolean
  {::rad.sql/schema :production
   ::rad.sql/tables #{"accounts"}
   ::rad.sql/column-name "active"})


(defattr account-addresses :account/addresses :ref
  {::attr/target         :com.example.model.address/id
   ::attr/cardinality    :many
   ::rad.sql/schema      :production
   ::rad.sql/tables      #{"addresses"}
   ::rad.sql/column-name "address_id"})


;; Derived data
(defattr account-locked? :account/locked? :boolean
  {})


(def account-attributes
  [account-id account-name account-active? account-locked?])


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; User


(defattr user-id :user/id :uuid
  {::rad.sql/schema :production
   ::rad.sql/tables #{"users"}})


(defattr user-name :user/name :string
  {::rad.sql/schema :production
   ::rad.sql/tables #{"users"}})


(def user-attributes [user-id user-name])


(def all-attributes
  (concat account-attributes user-attributes))
