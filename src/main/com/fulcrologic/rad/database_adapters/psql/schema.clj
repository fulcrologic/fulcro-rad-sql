(ns com.fulcrologic.rad.database-adapters.psql.schema
  (:require
    [camel-snake-kebab.core :as csk]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.fulcrologic.guardrails.core :refer [=> >defn]]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
    [next.jdbc.result-set :as rs]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log])
  (:import
    (java.sql Array)))

(extend-protocol rs/ReadableColumn
  Array
  (read-column-by-label [^Array v _] (set (.getArray v)))
  (read-column-by-index [^Array v _ _] (set (.getArray v))))

(>defn table-name
  "Get the table name for a given identity key"
  ([key->attribute {::attr/keys    [qualified-key identity? identities]
                    ::rad.sql/keys [table] :as attr}]
   [map? ::attr/attribute => string?]
   (if identity?
     (table-name attr)
     (let [identity (first identities)
           id-attr  (key->attribute identity)]
       (when-not (= 1 (count identities))
         (throw (ex-info "Cannot derive table name from ::attr/identities because there is more than one."
                  {:attr attr
                   :k    qualified-key})))
       (table-name id-attr))))
  ([{::attr/keys    [identity? qualified-key]
     ::rad.sql/keys [table]}]
   [::attr/attribute => string?]
   (when-not identity?
     (throw (ex-info "You must use an id-attribute with table-name" {:non-id-key qualified-key})))
   (or table (some-> qualified-key namespace csk/->snake_case))))

(defn attr->table-name
  "DEPRECATED. use `table-name` on an id attr. This one cannot be correct, since an attr can be on more than one tbl"
  ([k->attr {:keys [::attr/identities ::rad.sql/table]}]
   (or table (get-in k->attr [(first identities) ::rad.sql/table]))))

(defn column-name
  "Get the column name for the given attribute."
  ([k->attr {::attr/keys [identities cardinality type] :as attr}]
   (or
     (::rad.sql/column-name attr)
     (if (and (= :many cardinality) (= :ref type))
       (do
         (when-not (= 1 (count identities))
           (throw (ex-info "Cannot calculating column name that has multiple identities." {:attr attr})))
         (enc/if-let [reverse-target-attr (k->attr (first identities))
                      rev-target-table    (table-name k->attr reverse-target-attr)
                      rev-target-column   (column-name reverse-target-attr)
                      origin-table        (table-name k->attr attr)
                      origin-column       (some-> attr ::attr/qualified-key name csk/->snake_case)]
           ;; account_addresses_account_id
           (str/join "_" [origin-table origin-column rev-target-table rev-target-column])))
       (column-name attr))))
  ([{::attr/keys    [qualified-key cardinality type]
     ::rad.sql/keys [column-name]}]
   (when (and (= :many cardinality) (= :ref type))
     (throw (ex-info "Cannot calculate column name for to-many ref without k->attr." {:attr qualified-key})))
   (or
     column-name
     (some-> qualified-key name csk/->snake_case))))

(defn sequence-name [id-attribute]
  (str (table-name id-attribute) "_" (column-name id-attribute) "_seq"))

(def attr->column-name column-name)

(defn tables-and-columns
  "Return a sequence of [table-name column-name] that the given attribute appears at."
  [key->attribute {::attr/keys [identity? identities] :as attribute}]
  [(s/map-of qualified-keyword? ::attr/attribute) ::attr/attribute => (s/coll-of (s/tuple string? string?))]
  (let [col-name (column-name key->attribute attribute)]
    (if identity?
      [[(table-name attribute) col-name]]
      (mapv
        (fn [id-key]
          (let [id-attribute (key->attribute id-key)]
            [(table-name id-attribute) col-name]))
        identities))))

(defn attrs->sql-col-index
  "Takes a list of rad attributes and returns an index of the form
  `{[table column] :qualified/keyword}`"
  [k->attr]
  (into {}
    (mapcat
      (fn [attr] (map (fn [k] [k attr]) (tables-and-columns k->attr attr)))
      (vals k->attr))))

(def type-map
  {:string   "text"
   :password "text"
   :boolean  "BOOLEAN"
   :int      "INTEGER"
   :short    "SMALLINT"
   :long     "BIGINT"
   :decimal  "decimal(20,2)"
   :double   "DOUBLE PRECISION"
   :instant  "TIMESTAMP WITH TIME ZONE"
   :inst     "BIGINT"
   :enum     "INT"                                          ; These will be interned
   :keyword  "INT"
   :symbol   "INT"
   :uuid     "UUID"})

(>defn sql-type [{::attr/keys    [type cardinality]
                  ::rad.sql/keys [data-type max-length]}]
  [::attr/attribute => string?]
  (cond-> (if-let [result data-type]
            result
            (if (#{:string :password} type)
              (if max-length
                (str "VARCHAR(" max-length ")")
                (get type-map :string))
              (if-let [result (get type-map type)]
                result
                (do
                  (log/error "Unsupported type" type)
                  "TEXT"))))
    (= :many cardinality) (str "[]")))

(>defn new-table [table]
  [string? => map?]
  {:type :table :table table})

(>defn new-scalar-column
  [table column attr]
  [string? string? ::attr/attribute => map?]
  {:type :column :table table :column column :attr attr})

(>defn new-id-column [table column attr]
  [string? string? ::attr/attribute => map?]
  {:type :id :table table :column column :attr attr})

(>defn new-ref-column [table column attr]
  [string? string? ::attr/attribute => map?]
  {:type :ref :table table :column column :attr attr})

(defmulti op->sql (fn [k->attr {:keys [type]}] type))

(defmethod op->sql :table [_ {:keys [table]}] (format "CREATE TABLE IF NOT EXISTS %s ();\n" table))

(defmethod op->sql :ref [k->attr {:keys [table column attr]}]
  (let [{::attr/keys [cardinality target identities qualified-key]} attr
        target-attr (k->attr target)]
    (if (= :many cardinality)
      (do
        (when (not= 1 (count identities))
          (throw (ex-info "Reference column must have exactly 1 ::attr/identities entry." {:k qualified-key})))
        (enc/if-let [reverse-target-attr (k->attr (first identities))
                     rev-target-table    (sql.schema/table-name k->attr reverse-target-attr)
                     rev-target-column   (sql.schema/column-name reverse-target-attr)
                     table               (sql.schema/table-name k->attr target-attr)
                     column              (sql.schema/column-name k->attr attr)
                     index-name          (str column "_idx")]
          (str
            (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s) DEFERRABLE INITIALLY DEFERRED;\n"
              table column (sql-type reverse-target-attr) rev-target-table rev-target-column)
            (format "CREATE INDEX IF NOT EXISTS %s ON %s(%s);\n"
              index-name table column))
          (throw (ex-info "Cannot create to-many reference column." {:k qualified-key}))))
      (enc/if-let [origin-table  (sql.schema/table-name k->attr attr)
                   origin-column (sql.schema/column-name attr)
                   target-table  (sql.schema/table-name k->attr target-attr)
                   target-column (sql.schema/column-name target-attr)
                   target-type   (sql-type target-attr)
                   index-name    (str column "_idx")]
        (str
          (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s) DEFERRABLE INITIALLY DEFERRED;\n"
            origin-table origin-column target-type target-table target-column)
          (format "CREATE INDEX IF NOT EXISTS %s ON %s(%s);\n"
            index-name table column))
        (throw (ex-info "Cannot create to-many reference column." {:k qualified-key}))))))

(defmethod op->sql :id [k->attr {:keys [table column attr]}]
  (let [{::attr/keys [type]} attr
        index-name    (str table "_" column "_idx")
        sequence-name (str table "_" column "_seq")
        typ           (sql-type attr)]
    (str
      (if (#{:int :long} type)
        (str
          (format "CREATE SEQUENCE IF NOT EXISTS %s;\n" sequence-name)
          (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s PRIMARY KEY DEFAULT nextval('%s');\n"
            table column typ sequence-name))
        (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s PRIMARY KEY;\n"
          table column typ)))))

(defmethod op->sql :column [key->attr {:keys [table column attr]}]
  (let [{::attr/keys [type enumerated-values]} attr]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;\n" table column (sql-type attr))))

(defn attr->ops [schema-name key->attribute {::attr/keys [qualified-key type identity? identities]
                                             :keys       [::attr/schema]
                                             :as         attr}]
  (when (= schema schema-name)
    (enc/if-let [tables-and-columns (seq (sql.schema/tables-and-columns key->attribute attr))]
      (reduce
        (fn [s [table col]]
          (-> s
            (conj (new-table table))
            (conj (cond
                    identity? (new-id-column table col attr)
                    (= :ref type) (new-ref-column table col attr)
                    :else (new-scalar-column table col attr)))))
        []
        tables-and-columns)
      (log/error "Correct schema for attribute, but generation failed: "
        (::attr/qualified-key attr)
        (when (nil? (sql-type attr))
          (str " (No mapping for type " type ")"))))))

(>defn automatic-schema
  "Returns SQL schema for all attributes that support it."
  [schema-name attributes]
  [keyword? ::attr/attributes => (s/coll-of string?)]
  (let [key->attribute (attr/attribute-map attributes)
        db-ops         (mapcat (partial attr->ops schema-name key->attribute) attributes)
        {:keys [id table column ref]} (group-by :type db-ops)
        op             (partial op->sql key->attribute)
        new-tables     (mapv op (set table))
        new-ids        (mapv op (set id))
        new-columns    (mapv op (set column))
        new-refs       (mapv op (set ref))]
    (vec (concat new-tables new-ids new-columns new-refs))))

(comment
  (automatic-schema :production simplymeet.model-rad.model/all-attributes)
  )
