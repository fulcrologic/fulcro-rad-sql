(ns com.fulcrologic.rad.database-adapters.sql.resolvers
  (:require
    [clojure.pprint :refer [pprint]]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.attributes :as rad.attr]
    [com.fulcrologic.rad.form :as rad.form]
    [com.fulcrologic.guardrails.core :refer [>defn => |]]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.query :as sql.query]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
    [com.wsscode.pathom.connect :as pc]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [next.jdbc.sql :as jdbc.sql]
    [clojure.spec.alpha :as s]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reads

(defn entity-query
  "The entity query used by the pathom resolvers."
  [{::rad.attr/keys [id-attribute key->attribute] :as env} input]
  (let [schema (::rad.sql/schema id-attribute)
        one?   (not (sequential? input))]
    (enc/if-let [data-source (get-in env [::rad.sql/connection-pools schema])
                 query*      (or
                               (get env :com.wsscode.pathom.core/parent-query)
                               (get env ::rad.sql/default-query))]
      (let [result (sql.query/eql-query env data-source query* input)]
          (if one?
            (first result)
            result))
      (log/info "Unable to complete query."))))


(defn id-resolver [{::rad.attr/keys [id-attribute attributes k->attr]}]
  (enc/if-let [id-key  (::rad.attr/qualified-key id-attribute)
               outputs (rad.attr/attributes->eql attributes)
               schema  (::rad.sql/schema id-attribute)]
    {::pc/sym     (symbol
                    (str (namespace id-key))
                    (str (name id-key) "-resolver"))
     ::pc/output  outputs
     ::pc/batch?  true
     ::pc/resolve (fn [env input]
                    (auth/redact env
                      (entity-query
                        (assoc env
                          ::rad.attr/id-attribute id-attribute
                          ::rad.sql/schema schema
                          ::rad.sql/default-query outputs)
                        input)))
     ::pc/input   #{id-key}}
    (log/error
      "Unable to generate id-resolver. Attribute was missing schema, "
      "or could not be found" (::rad.attr/qualified-key id-attribute))))


(defn generate-resolvers
  "Returns a sequence of resolvers that can resolve attributes from
  SQL databases."
  [attributes schema]
  (log/info "Generating resolvers for SQL schema" schema)
  (let [k->attr             (enc/keys-by ::rad.attr/qualified-key attributes)
        id-attr->attributes (->> attributes
                              (filter #(= schema (::rad.sql/schema %)))
                              (mapcat
                                (fn [attribute]
                                  (for [entity-id (::rad.attr/identities attribute)]
                                    (assoc attribute ::entity-id (k->attr entity-id)))))
                              (group-by ::entity-id))]
    (log/info "Generating resolvers")
    (reduce-kv
      (fn [resolvers id-attr attributes]
        (log/info "Generating resolver for id key" (::rad.attr/qualified-key id-attr)
          "to resolve" (mapv ::rad.attr/qualified-key attributes))
        (conj resolvers (id-resolver {::rad.attr/id-attribute id-attr
                                      ::rad.attr/attributes   attributes
                                      ::rad.attr/k->attr      k->attr})))
      [] id-attr->attributes)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Writes

(>defn attr-table [key->attribute k]
  [(s/map-of qualified-keyword? ::rad.attr/attribute) keyword? => string?]
  (->>
    (key->attribute k)
    (sql.schema/attr->table-name key->attribute)))

(defn delta->txs
  "Given a save form delta, returns the list describing the steps
  needed to be done to persist the form."
  [key->attribute form-delta]
  (for [[[id-k id] entity-diff] form-delta
        :let [attr             (key->attribute id-k)
              table            (sql.schema/attr->table-name key->attribute attr)
              persistent-attrs (filter
                                 (fn [[k v]]
                                   (and
                                     (= table (attr-table key->attribute k))
                                     (not= (:before v) (:after v))))
                                 entity-diff)]
        :when (and table (seq persistent-attrs))]
    {::rad.sql/table  table
     ::rad.sql/schema (::rad.sql/schema attr)
     :tx/action       :sql/update
     :tx/attrs        (enc/map-vals :after entity-diff)
     :tx/where        {id-k id}}))

(defn save-form!
  "Does all the necessary operations to persist mutations from the
  form delta into the appropriate tables in the appropriate databases"
  [{::rad.attr/keys [key->attribute]
    ::rad.sql/keys  [connection-pools]
    :as             env} {::rad.form/keys [delta]}]
  (log/info "SQL Save of delta " (with-out-str (pprint delta)))
  (doseq [{:tx/keys       [attrs where]
           ::rad.sql/keys [schema table]
           :as            tx} (delta->txs key->attribute delta)]
    (log/info "SQL steps: " (with-out-str (pprint tx)))
    (enc/if-let [schema-pool (get connection-pools schema)
                 db          (get-in env [::rad.sql/databases schema])]
      (jdbc.sql/update! (:datasource db) table attrs where)
      (throw (ex-info "No connection found for SQL transaction"
               {:type ::rad.sql/missing-connection
                :tx   tx})))))

(defn delete-entity! [env params]
  (log/error "DELETE NOT IMPLEMENTED" params))
