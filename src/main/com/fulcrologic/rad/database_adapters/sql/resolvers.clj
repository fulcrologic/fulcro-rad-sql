(ns com.fulcrologic.rad.database-adapters.sql.resolvers
  (:require
    [com.wsscode.pathom.connect                      :as pc]
    [com.fulcrologic.rad.attributes                  :as attr]
    [com.fulcrologic.rad.database-adapters.sql.query :as sql.query]
    [com.fulcrologic.rad.authorization               :as auth]
    [com.fulcrologic.rad.form                        :as rad.form]
    [com.fulcrologic.rad.database-adapters.sql       :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.utils :as u]
    [taoensso.encore                                 :as enc]
    [taoensso.timbre                                 :as log]
    [next.jdbc.sql                                   :as jdbc.sql]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reads

;; TODO: This is nearly identical to the Datomic one...helper function
;; would be nice
(defn id-resolver [id-attr attributes]
  (enc/if-let [id-key        (::attr/qualified-key id-attr)
               outputs       (attr/attributes->eql attributes)
               schema        (::rad.sql/schema id-attr)]
    {::pc/sym     (symbol
                    (str (namespace id-key))
                    (str (name id-key) "-resolver"))
     ::pc/output  outputs
     ::pc/batch?  true
     ::pc/resolve (fn [env input] (->>
                                    (sql.query/entity-query
                                      (assoc env
                                        ::rad.sql/attributes attributes
                                        ::rad.sql/id-attribute id-attr
                                        ::rad.sql/schema schema
                                        ::attr/qualified-key id-key
                                        ::rad.sql/default-query outputs)
                                      input)
                                    (auth/redact env)))
     ::pc/input   #{id-key}}
    (log/error
      "Unable to generate id-resolver. Attribute was missing schema, or "
      "could not be found in the attribute registry: " id-attr)))


(defn generate-resolvers
  "Returns a sequence of resolvers that can resolve attributes from
  SQL databases."
  [attributes schema]
  (let [attributes          (filter #(= schema (::rad.sql/schema %)) attributes)
        table->id-attribute (reduce
                              (fn [m {::attr/keys [identity?]
                                      ::rad.sql/keys      [tables] :as attr}]
                                (if identity?
                                  (reduce
                                    (fn [m2 t] (assoc m2 t attr))
                                    m
                                    tables)
                                  m))
                              {} attributes)
        id-attr->attributes (->> attributes
                              (mapcat
                                (fn [attribute]
                                  (for [table (get attribute ::rad.sql/tables)
                                        :let [id-attr (table->id-attribute table)]]
                                    (assoc attribute ::rad.sql/k id-attr))))
                              (group-by ::rad.sql/k))]
    (reduce-kv
      (fn [resolvers id-attr attributes]
        (conj resolvers (id-resolver id-attr attributes)))
      [] id-attr->attributes)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Writes

(def attr-table (comp #'u/attr->table-name #'attr/key->attribute))


(defn delta->txs
  "Given a save form delta, returns the list describing the steps
  needed to be done to persist the form."
  [form-delta]
  (for [[[id-k id] entity-diff] form-delta
        :let [attr (attr/key->attribute id-k)
              table (u/attr->table-name attr)
              persistent-attrs (filter
                                 (fn [[k v]]
                                   (and
                                     (= table (attr-table k))
                                     (not= (:before v) (:after v))))
                                 entity-diff)]
        :when (and table (seq persistent-attrs))]
    {::rad.sql/table    table
     ::rad.sql/schema   (::rad.sql/schema attr)
     :tx/action :sql/update
     :tx/attrs  (enc/map-vals :after entity-diff)
     :tx/where  {id-k id}}))


(defn save-form!
  "Does all the necessary operations to persist mutations from the
  form delta into the appropriate tables in the appropriate databases"
  [env {::rad.form/keys [delta]}]
  (doseq [{:tx/keys [attrs where]
           ::rad.sql/keys [schema table]
           :as tx} (delta->txs delta)]
    (if-let [db (get-in env [::rad.sql/databases schema])]
      (jdbc.sql/update! (:datasource db) table attrs where)
      (throw (ex-info "No connection found for SQL transaction"
               {:type ::rad.sql/missing-connection
                :tx tx})))))
