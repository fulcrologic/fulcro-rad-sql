(ns com.fulcrologic.rad.database-adapters.sql.migration
  (:require
    [clojure.pprint :refer [pprint]]
    [next.jdbc :as jdbc]
    [clojure.string :as str]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.guardrails.core :refer [>defn =>]]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
    [clojure.spec.alpha :as s])
  (:import (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariDataSource)))

(def type-map
  {:string   "VARCHAR(2048)"
   :password "VARCHAR(512)"
   :boolean  "BOOLEAN"
   :int      "INTEGER"
   :long     "BIGINT"
   :decimal  "decimal(20,2)"
   :instant  "TIMESTAMP WITH TIME ZONE"
   :inst     "BIGINT"
   ;; There is no standard SQL enum, and many ppl think they are a bad idea in general. Given
   ;; that we have other ways of enforcing them we use a standard type instead.
   :enum     "VARCHAR(200)"
   :keyword  "VARCHAR(200)"
   :symbol   "VARCHAR(200)"
   :uuid     "UUID"})

(>defn sql-type [{::attr/keys    [type]
                  ::rad.sql/keys [max-length]}]
  [::attr/attribute => string?]
  (if (#{:string :password :keyword :symbol} type)
    (if max-length
      (str "VARCHAR(" max-length ")")
      "VARCHAR(200)")
    (if-let [result (get type-map type)]
      result
      (do
        (log/error "Unsupported type" type)
        "TEXT"))))

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

(defmulti op->sql (fn [k->attr adapter {:keys [type]}] type))

(defmethod op->sql :table [_ adapter {:keys [table]}] (format "CREATE TABLE IF NOT EXISTS %s ();\n" table))

(defmethod op->sql :ref [k->attr adapter {:keys [table column attr]}]
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
            (vendor/add-referential-column-statement adapter
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
          (vendor/add-referential-column-statement adapter
            origin-table origin-column target-type target-table target-column)
          (format "CREATE INDEX IF NOT EXISTS %s ON %s(%s);\n"
            index-name table column))
        (throw (ex-info "Cannot create to-many reference column." {:k qualified-key}))))))

(defmethod op->sql :id [k->attr adapter {:keys [table column attr]}]
  (let [{::attr/keys [type]} attr
        index-name    (str table "_" column "_idx")
        sequence-name (str table "_" column "_seq")
        typ           (sql-type attr)]
    (str
      (if (#{:int :long} type)
        (str
          (format "CREATE SEQUENCE IF NOT EXISTS %s;\n" sequence-name)
          (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s DEFAULT nextval('%s');\n"
            table column typ sequence-name))
        (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;\n"
          table column typ sequence-name))
      (format "CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(%s);\n"
        index-name table column))))

(defmethod op->sql :column [key->attr adapter {:keys [table column attr]}]
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
  [schema-name adapter attributes]
  [keyword? ::vendor/adapter ::attr/attributes => (s/coll-of string?)]
  (let [key->attribute (attr/attribute-map attributes)
        db-ops         (mapcat (partial attr->ops schema-name key->attribute) attributes)
        {:keys [id table column ref]} (group-by :type db-ops)
        op             (partial op->sql key->attribute adapter)
        new-tables     (mapv op (set table))
        new-ids        (mapv op (set id))
        new-columns    (mapv op (set column))
        new-refs       (mapv op (set ref))]
    (vec (concat new-tables new-ids new-columns new-refs))))

(defn migrate! [config all-attributes connection-pools]
  (let [database-map (some-> config ::rad.sql/databases)]
    (doseq [[dbkey dbconfig] database-map]
      (let [{:sql/keys    [auto-create-missing? schema vendor]
             :flyway/keys [migrate? migrations]} dbconfig
            ^HikariDataSource pool (get connection-pools dbkey)
            db                     {:datasource pool}
            adapter                (case vendor
                                     :postgresql (vendor/->PostgreSQLAdapter)
                                     (vendor/->H2Adapter))]
        (if pool
          (cond
            (and migrate? (seq migrations))
            (do
              (log/info (str "Processing Flywawy migrations for " dbkey))
              (let [flyway (Flyway.)]
                (log/info "Migration location is set to: " migrations)
                (.setLocations flyway (into-array String migrations))
                (.setDataSource flyway pool)
                (.setBaselineOnMigrate flyway true)
                (.migrate flyway)))

            auto-create-missing?
            (let [stmts (automatic-schema schema adapter all-attributes)]
              (log/info "Automatically trying to create SQL schema from attributes.")
              (doseq [s stmts]
                (try
                  (jdbc/execute! pool [s])
                  (catch Exception e
                    (log/error e s)
                    (throw e))))))
          (log/error (str "No pool for " dbkey ". Skipping migrations.")))))))
