(ns com.fulcrologic.rad.database-adapters.sql.resolvers
  (:require
    [clojure.pprint :refer [pprint]]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.form :as rad.form]
    [com.fulcrologic.guardrails.core :refer [>defn => |]]
    [com.fulcrologic.rad.database-adapters.sql :as rsql]
    [com.fulcrologic.rad.database-adapters.sql.query :as sql.query]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
    [com.wsscode.pathom.connect :as pc]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [next.jdbc.sql :as jdbc.sql]
    [clojure.spec.alpha :as s]
    [next.jdbc :as jdbc]
    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.ids :as ids]
    [clojure.string :as str]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reads

(defn entity-query
  "The entity query used by the pathom resolvers."
  [{::attr/keys [id-attribute key->attribute] :as env} input]
  (enc/if-let [query* (get env ::rsql/default-query)]
    (let [result (sql.query/eql-query! env id-attribute query* input)]
      result)
    (do
      (log/info "Unable to complete query.")
      nil)))


(defn id-resolver [{::attr/keys [id-attribute attributes k->attr]}]
  (enc/if-let [id-key  (::attr/qualified-key id-attribute)
               outputs (attr/attributes->eql attributes)
               schema  (::attr/schema id-attribute)]
    {::pc/sym     (symbol
                    (str (namespace id-key))
                    (str (name id-key) "-resolver"))
     ::pc/output  outputs
     ::pc/batch?  true
     ::pc/resolve (fn [env input]
                    (auth/redact env
                      (entity-query
                        (assoc env
                          ::attr/id-attribute id-attribute
                          ::attr/schema schema
                          ::rsql/default-query outputs)
                        input)))
     ::pc/input   #{id-key}}
    (log/error
      "Unable to generate id-resolver. Attribute was missing schema, "
      "or could not be found" (::attr/qualified-key id-attribute))))


(defn generate-resolvers
  "Returns a sequence of resolvers that can resolve attributes from
  SQL databases."
  [attributes schema]
  (log/info "Generating resolvers for SQL schema" schema)
  (let [k->attr             (enc/keys-by ::attr/qualified-key attributes)
        id-attr->attributes (->> attributes
                              (filter #(= schema (::attr/schema %)))
                              (mapcat
                                (fn [attribute]
                                  (for [entity-id (::attr/identities attribute)]
                                    (assoc attribute ::entity-id (k->attr entity-id)))))
                              (group-by ::entity-id))]
    (log/info "Generating resolvers")
    (reduce-kv
      (fn [resolvers id-attr attributes]
        (log/info "Generating resolver for id key" (::attr/qualified-key id-attr)
          "to resolve" (mapv ::attr/qualified-key attributes))
        (conj resolvers (id-resolver {::attr/id-attribute id-attr
                                      ::attr/attributes   attributes
                                      ::attr/k->attr      k->attr})))
      [] id-attr->attributes)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Writes

(def keys-in-delta
  (fn keys-in-delta [delta]
    (let [id-keys  (into #{}
                     (map first)
                     (keys delta))
          all-keys (into id-keys
                     (mapcat keys)
                     (vals delta))]
      all-keys)))

(defn schemas-for-delta [{::attr/keys [key->attribute]} delta]
  (let [all-keys (keys-in-delta delta)
        schemas  (into #{}
                   (keep #(-> % key->attribute ::attr/schema))
                   all-keys)]
    schemas))

(defn- generate-tempids [ds key->attribute delta]
  (reduce
    (fn [result [table id]]
      (if (tempid/tempid? id)
        (let [{::attr/keys [type] :as id-attr} (key->attribute table)
              real-id (if (#{:int :long} type)
                        (:ID (first (jdbc/execute! ds [(format "SELECT NEXTVAL('%s') AS id" (sql.schema/sequence-name id-attr))])))
                        (ids/new-uuid))]
          (assoc result id real-id))
        result))
    {}
    (keys delta)))

(defn- table-local-attr
  "Returns an attribute or nil if it isn't stored on the semantic table of the attribute."
  [k->a k]
  (let [{::attr/keys [type cardinality] :as attr} (k->a k)]
    (when (not= :ref type)
      attr)))

(defn scalar-insert
  [{::attr/keys [key->attribute] :as env} tempids [table id :as ident] diff]
  (when (tempid/tempid? id)
    (let [{::attr/keys [type] :as id-attr} (key->attribute table)
          table-name    (sql.schema/table-name key->attribute id-attr)
          real-id       (get tempids id id)
          scalar-attrs  (keep
                          (fn [k] (table-local-attr key->attribute k))
                          (keys diff))
          new-val       (fn [{::attr/keys [qualified-key]}] (let [v (get-in diff [qualified-key :after])]
                                                              (when-not (nil? v)
                                                                (sql.query/q v))))
          column-names  (str/join "," (into [(sql.schema/column-name id-attr)]
                                        (keep (fn [attr]
                                                (when-not (nil? (new-val attr))
                                                  (sql.schema/column-name attr))))
                                        scalar-attrs))
          column-values (str/join "," (into [(sql.query/q real-id)]
                                        (keep new-val)
                                        scalar-attrs))]
      (format "INSERT INTO %s (%s) VALUES (%s)" table-name column-names column-values))))

(defn delta->scalar-inserts [{::attr/keys [key->attribute]
                              ::rsql/keys [connection-pools]
                              :as         env} schema delta]
  (let [ds      (get connection-pools schema)
        tempids (generate-tempids ds key->attribute delta)
        stmts   (keep (fn [[ident diff]] (scalar-insert env tempids ident diff)) delta)]
    {:tempids        tempids
     :insert-scalars stmts}))

(defn delta->scalar-updates [{::attr/keys [key->attribute]
                              ::rsql/keys [connection-pools]
                              :as         env} schema delta]
  (let [ds      (get connection-pools schema)
        tempids (generate-tempids ds key->attribute delta)
        stmts   (keep (fn [[ident diff]] (scalar-insert env tempids ident diff)) delta)]
    {:tempids        tempids
     :insert-scalars stmts}))
(defn save-form!
  "Does all the necessary operations to persist mutations from the
  form delta into the appropriate tables in the appropriate databases"
  [{::attr/keys [key->attribute]
    ::rsql/keys [connection-pools]
    :as         env} {::rad.form/keys [delta]}]
  (let [schemas (schemas-for-delta env delta)
        result  (atom {:tempids {}})]
    (log/debug "Saving form across " schemas)
    (log/debug "SQL Save of delta " (with-out-str (pprint delta)))
    ;; TASK: Transaction should be opened on all databases at once, so that they all succeed or fail
    (doseq [schema (keys connection-pools)]
      (let [ds             (get connection-pools schema)
            {:keys [tempids insert-scalars]} (delta->scalar-inserts env schema delta) ; any non-fk column with a tempid
            update-scalars (delta->scalar-updates env schema delta) ; any non-fk columns on entries with pre-existing id
            update-refs    [] #_(delta->ref-updates env tempids schema delta) ; all fk columns on entire delta
            steps          (concat insert-scalars update-scalars update-refs)]
        (jdbc/with-transaction [ds ds {:isolation :serializable}]
          (doseq [sql steps]
            (log/debug sql)
            (jdbc/execute! ds [sql])))
        (swap! result update :tempids merge tempids)))
    @result))

(defn delete-entity! [env params]
  (log/error "DELETE NOT IMPLEMENTED" params))
