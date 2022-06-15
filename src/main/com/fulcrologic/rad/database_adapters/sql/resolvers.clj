(ns com.fulcrologic.rad.database-adapters.sql.resolvers
  (:require
    [clojure.pprint :refer [pprint]]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.form :as rad.form]
    [com.fulcrologic.rad.options-util :refer [?!]]
    [com.fulcrologic.guardrails.core :refer [>defn => |]]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.query :as sql.query]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [next.jdbc.sql :as jdbc.sql]
    [clojure.spec.alpha :as s]
    [next.jdbc :as jdbc]

    ;; IMPORTANT: This turns on instant coercion:
    [next.jdbc.date-time]

    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.ids :as ids]
    [clojure.string :as str]
    [clojure.set :as set]
    [edn-query-language.core :as eql]
    [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reads

(defn entity-query
  "The entity query used by the pathom resolvers."
  [{::attr/keys [id-attribute key->attribute] :as env} input]
  (enc/if-let [query* (get env ::rad.sql/default-query)]
    (let [result (sql.query/eql-query! env id-attribute query* input)]
      result)
    (do
      (log/info "Unable to complete query.")
      nil)))

(defn id-resolver [{::attr/keys [id-attribute attributes k->attr]}]
  (enc/if-let [id-key  (::attr/qualified-key id-attribute)
               outputs (attr/attributes->eql attributes)
               schema  (::attr/schema id-attribute)]
    (let [transform (::pc/transform id-attribute)]
      (cond-> {:com.wsscode.pathom.connect/sym     (symbol
                                                     (str (namespace id-key))
                                                     (str (name id-key) "-resolver"))
               :com.wsscode.pathom.connect/output  outputs
               :com.wsscode.pathom.connect/batch?  true
               :com.wsscode.pathom.connect/resolve (fn [env input]
                                                     (auth/redact env
                                                       (log/spy :trace (entity-query
                                                                         (assoc env
                                                                           ::attr/id-attribute id-attribute
                                                                           ::attr/schema schema
                                                                           ::rad.sql/default-query outputs)
                                                                         (log/spy :trace input)))))
               :com.wsscode.pathom.connect/input   #{id-key}}
        transform transform))
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

(defn resolve-tempid-in-value [tempids v]
  (cond
    (tempid/tempid? v) (get tempids v v)
    (eql/ident? v) (let [[k id] v]
                     (if (tempid/tempid? id)
                       [k (get tempids id id)]
                       v))
    :else v))

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
                        (:id (first (jdbc/execute! ds [(format "SELECT NEXTVAL('%s') AS id" (sql.schema/sequence-name id-attr))])))
                        (ids/new-uuid))]
          (assoc result id real-id))
        result))
    {}
    (keys delta)))

(defn- table-local-attr
  "Returns an attribute or nil if it isn't stored on the semantic table of the attribute."
  [k->a schema-to-save k]
  (let [{::attr/keys [type cardinality schema] :as attr} (k->a k)]
    (when (= schema schema-to-save)
      (when-not (and (= cardinality :many) (= :ref type))
        attr))))

(defn form->sql-value [{::attr/keys    [type cardinality]
                        ::rad.sql/keys [form->sql-value]} form-value]
  (cond
    (and (= :ref type) (not= :many cardinality) (eql/ident? form-value)) (second form-value)
    form->sql-value (form->sql-value form-value)
    (= type :enum) (str form-value)
    :else form-value))

(defn scalar-insert
  [{::attr/keys [key->attribute] :as env} schema-to-save tempids [table id :as ident] diff]
  (when (tempid/tempid? (log/spy :trace id))
    (let [{::attr/keys [type schema] :as id-attr} (key->attribute table)]
      (if (= schema schema-to-save)
        (let [table-name    (sql.schema/table-name key->attribute id-attr)
              real-id       (get tempids id id)
              scalar-attrs  (keep
                              (fn [k] (table-local-attr key->attribute schema-to-save k))
                              (keys diff))
              new-val       (fn [{::attr/keys [qualified-key schema] :as attr}]
                              (when (= schema schema-to-save)
                                (let [v (get-in diff [qualified-key :after])
                                      v (resolve-tempid-in-value tempids v)]
                                  (form->sql-value attr v))))
              column-names  (str/join "," (into [(sql.schema/column-name id-attr)]
                                            (keep (fn [attr]
                                                    (when-not (nil? (new-val attr))
                                                      (sql.schema/column-name attr))))
                                            scalar-attrs))
              column-values (into [real-id] (keep new-val) scalar-attrs)
              placeholders  (str/join "," (repeat (count column-values) "?"))]
          (into [(format "INSERT INTO %s (%s) VALUES (%s)" table-name column-names placeholders)] column-values))
        (log/debug "Schemas do not match. Not updating" ident)))))

(defn delta->scalar-inserts [{::attr/keys    [key->attribute]
                              ::rad.sql/keys [connection-pools]
                              :as            env} schema delta]
  (let [ds      (get connection-pools schema)
        tempids (log/spy :trace (generate-tempids ds key->attribute delta))
        stmts   (keep (fn [[ident diff]] (scalar-insert env schema tempids ident diff)) delta)]
    {:tempids        tempids
     :insert-scalars stmts}))

(defn scalar-update
  [{::keys      [tempids]
    ::attr/keys [key->attribute] :as env} schema-to-save [table id :as ident] diff]
  (when-not (tempid/tempid? id)
    (let [{::attr/keys [type schema] :as id-attr} (key->attribute table)]
      (when (= schema-to-save schema)
        (let [table-name   (sql.schema/table-name key->attribute id-attr)
              scalar-attrs (keep
                             (fn [k] (table-local-attr key->attribute schema-to-save k))
                             (keys diff))
              old-val      (fn [{::attr/keys [qualified-key] :as attr}]
                             (some->> (get-in diff [qualified-key :before])
                               (form->sql-value attr)))
              new-val      (fn [{::attr/keys [qualified-key schema] :as attr}]
                             (when (= schema schema-to-save)
                               (let [v (get-in diff [qualified-key :after])
                                     v (resolve-tempid-in-value tempids v)]
                                 (form->sql-value attr v))))
              {:keys [columns values]} (reduce
                                         (fn [{:keys [columns values] :as result} attr]
                                           (let [new      (log/spy :trace (new-val attr))
                                                 col-name (sql.schema/column-name attr)
                                                 old      (old-val attr)]
                                             (cond
                                               (and old (nil? new))
                                               (-> result
                                                 (update :columns conj col-name)
                                                 (update :values conj nil))

                                               (not (nil? new))
                                               (-> result
                                                 (update :columns conj col-name)
                                                 (update :values conj new))

                                               :else
                                               result)))
                                         {:columns []
                                          :values  []}
                                         scalar-attrs)
              placeholders (str/join "," (map #(str % " = ?") columns))]
          (when (seq columns)
            (into
              [(format "UPDATE %s SET %s WHERE %s = ?" table-name placeholders (sql.schema/column-name id-attr))]
              (conj values id))))))))

(defn delta->scalar-updates [env schema delta]
  (let [stmts (vec (keep (fn [[ident diff]] (scalar-update env schema ident diff)) delta))]
    stmts))

(defn- ref-one-attr
  [k->a k]
  (let [{::attr/keys [type cardinality] :as attr} (k->a k)]
    (when (and (= :ref type) (not= cardinality :many))
      attr)))

(defn- ref-many-attr
  [k->a k]
  (let [{::attr/keys [type cardinality] :as attr} (k->a k)]
    (when (and (= :ref type) (= cardinality :many))
      attr)))

(defn to-one-ref-update [{::attr/keys [key->attribute] :as env} schema-to-save id-attr tempids row-id attr old-val [_ new-id :as new-val]]
  (when (= schema-to-save (::attr/schema attr))
    (let [table-name     (sql.schema/table-name key->attribute id-attr)
          {delete-on-remove? ::rad.sql/delete-referent?} attr
          id-column-name (sql.schema/column-name id-attr)
          target-id      (get tempids row-id row-id)
          new-id         (get tempids new-id new-id)]
      (cond
        new-id
        (into [(format "UPDATE %s SET %s = ? WHERE %s = ?"
                 table-name (sql.schema/column-name attr) id-column-name)]
          [new-id target-id])

        old-val
        (if delete-on-remove?
          (let [foreign-table-id-key  (first old-val)
                old-id                (second old-val)
                foreign-table-id-attr (key->attribute foreign-table-id-key)
                foreign-table-name    (sql.schema/table-name key->attribute foreign-table-id-attr)
                foreign-id-column     (sql.schema/column-name key->attribute foreign-table-id-attr)]
            (into [(format "DELETE FROM %s WHERE %s = ?" foreign-table-name foreign-id-column)] [old-id]))
          (into [(format "UPDATE %s SET %s = NULL WHERE %s = ?"
                   table-name (sql.schema/column-name attr) id-column-name)]
            [target-id]))))))

(defn to-many-ref-update [{::attr/keys [key->attribute] :as env} schema target-row-id-attr tempids target-id to-many-attr old-val new-val]
  (enc/when-let [table-key     (and (not= old-val new-val) (ffirst (concat old-val new-val)))
                 right-schema? (= schema (::attr/schema to-many-attr))]
    (let [{delete-on-remove? ::rad.sql/delete-referent?} to-many-attr
          target-id       (get tempids target-id target-id)
          foreign-id-attr (key->attribute table-key)
          foreign-table   (sql.schema/table-name key->attribute foreign-id-attr)
          foreign-column  (sql.schema/column-name foreign-id-attr)
          column-name     (sql.schema/column-name key->attribute to-many-attr)
          old-ids         (into #{} (map (fn [[_ id]] (get tempids id id))) old-val)
          new-ids         (into #{} (map (fn [[_ id]] (get tempids id id))) new-val)
          adds            (set/difference new-ids old-ids)
          removes         (set/difference old-ids new-ids)
          add-stmts       (map (fn [id]
                                 [(format "UPDATE %s SET %s = ? WHERE %s = ?"
                                    foreign-table column-name foreign-column)
                                  target-id id]) adds)
          remove-stmts    (map (fn [id]
                                 (if delete-on-remove?
                                   [(format "DELETE FROM %s WHERE %s = ?"
                                      foreign-table foreign-column) id]
                                   [(format "UPDATE %s SET %s = NULL WHERE %s = ?"
                                      foreign-table column-name foreign-column) id]))
                            removes)]
      (concat add-stmts remove-stmts))))

(defn ref-updates [{::attr/keys [key->attribute] :as env} schema tempids [table id :as ident] diff]
  (let [id-attr           (key->attribute table)
        to-one-ref-attrs  (keep (fn [k] (ref-one-attr key->attribute k)) (keys diff))
        to-many-ref-attrs (keep (fn [k] (ref-many-attr key->attribute k)) (keys diff))
        old-val           (fn [{::attr/keys [qualified-key]}] (get-in diff [qualified-key :before]))
        new-val           (fn [{::attr/keys [qualified-key]}] (get-in diff [qualified-key :after]))]
    (concat
      (keep (fn [a] (to-one-ref-update env schema id-attr tempids id a (old-val a) (new-val a))) to-one-ref-attrs)
      (mapcat (fn [a] (to-many-ref-update env schema id-attr tempids id a (old-val a) (new-val a))) to-many-ref-attrs))))

(defn delta->ref-updates [env tempids schema delta]
  (let [stmts (vec (mapcat (fn [[ident diff]] (ref-updates env schema tempids ident diff)) delta))]
    stmts))

(defn save-form!
  "Does all the necessary operations to persist mutations from the
  form delta into the appropriate tables in the appropriate databases"
  [{::attr/keys    [key->attribute]
    ::rad.sql/keys [connection-pools adapters default-adapter]
    :as            env} {::rad.form/keys [delta]}]
  (let [schemas (schemas-for-delta env delta)
        result  (atom {:tempids {}})]
    (log/debug "Saving form across " schemas)
    (log/debug "SQL Save of delta " (with-out-str (pprint delta)))
    ;; TASK: Transaction should be opened on all databases at once, so that they all succeed or fail
    (doseq [schema (keys connection-pools)]
      (let [adapter        (get adapters schema default-adapter)
            ds             (get connection-pools schema)
            {:keys [tempids insert-scalars]} (log/spy :trace (delta->scalar-inserts env schema delta)) ; any non-fk column with a tempid
            update-scalars (log/spy :trace (delta->scalar-updates (assoc env ::tempids tempids) schema delta)) ; any non-fk columns on entries with pre-existing id
            update-refs    (log/spy :trace (delta->ref-updates env tempids schema delta)) ; all fk columns on entire delta
            steps          (concat insert-scalars update-scalars update-refs)]
        (jdbc/with-transaction [ds ds {:isolation :serializable}]
          ;; allow relaxed FK constraints until end of txn
          (when adapter
            (vendor/relax-constraints! adapter ds))
          (doseq [stmt-with-params steps]
            (log/debug stmt-with-params)
            (jdbc/execute! ds stmt-with-params)))
        (swap! result update :tempids merge tempids)))
    @result))

(defn delete-entity! [env params]
  (log/error "DELETE NOT IMPLEMENTED" params))
