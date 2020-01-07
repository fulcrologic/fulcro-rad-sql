(ns com.fulcrologic.rad.database-adapters.sql.resolvers
  (:require
    [com.fulcrologic.rad.attributes                   :as attr]
    [com.fulcrologic.rad.authorization                :as auth]
    [com.fulcrologic.rad.form                         :as rad.form]
    [com.fulcrologic.rad.database-adapters.sql        :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.query  :as sql.query]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
    [com.wsscode.pathom.connect                       :as pc]
    [taoensso.encore                                  :as enc]
    [taoensso.timbre                                  :as log]
    [next.jdbc.sql                                    :as jdbc.sql]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reads

(defn entity-query
  "The entity query used by the pathom resolvers."
  [{::rad.sql/keys [schema attributes id-attribute] :as env} input]
  (let [one? (not (sequential? input))]
    (enc/if-let [db     (get-in env [::rad.sql/databases schema])
                 query* (or
                          (get env :com.wsscode.pathom.core/parent-query)
                          (get env ::rad.sql/default-query))]
      ;; TODO entity input
      (let [result (sql.query/eql-query db query*
                     {::attr/attributes attributes
                      ::id-attribute id-attribute})]
        (if one?
          (first result)
          result))
      (log/info "Unable to complete query."))))


(defn id-resolver [id-attr attributes]
  (enc/if-let [id-key  (::attr/qualified-key id-attr)
               outputs (attr/attributes->eql attributes)
               schema  (::rad.sql/schema id-attr)]
    {::pc/sym     (symbol
                    (str (namespace id-key))
                    (str (name id-key) "-resolver"))
     ::pc/output  outputs
     ::pc/batch?  true
     ::pc/resolve (fn [env input]
                    (auth/redact env
                      (sql.query/entity-query
                        (assoc env
                          ::rad.sql/attributes attributes
                          ::rad.sql/id-attribute id-attr
                          ::rad.sql/schema schema
                          ::attr/qualified-key id-key
                          ::rad.sql/default-query outputs)
                        input)))
     ::pc/input   #{id-key}}
    (log/error
      "Unable to generate id-resolver. Attribute was missing schema, "
      "or could not be found" id-attr)))


(defn generate-resolvers
  "Returns a sequence of resolvers that can resolve attributes from
  SQL databases."
  [attributes schema]
  (let [attributes          (filter #(= schema (::rad.sql/schema %)) attributes)
        k->attr             (enc/keys-by ::attr/qualified-key attributes)
        id-attr->attributes (->> attributes
                              (mapcat
                                (fn [attribute]
                                  (for [entity-id (::rad.sql/entity-ids attribute)]
                                    (assoc attribute ::entity-id (k->attr entity-id)))))
                              (group-by ::entity-id))]
    (reduce-kv
      (fn [resolvers id-attr attributes]
        (conj resolvers (id-resolver id-attr attributes)))
      [] id-attr->attributes)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Writes

(def attr-table (comp #'sql.schema/attr->table-name #'attr/key->attribute))


(defn delta->txs
  "Given a save form delta, returns the list describing the steps
  needed to be done to persist the form."
  [form-delta]
  (for [[[id-k id] entity-diff] form-delta
        :let [attr (attr/key->attribute id-k)
              table (sql.schema/attr->table-name attr)
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
