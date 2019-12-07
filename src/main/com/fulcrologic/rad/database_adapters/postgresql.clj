(ns com.fulcrologic.rad.database-adapters.postgresql
  (:require
    [camel-snake-kebab.core :as csk]
    [clojure.java.jdbc :as jdbc]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.fulcrologic.guardrails.core :refer [>defn =>]]
    [com.fulcrologic.rad.attributes :as attr]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [taoensso.timbre :as timbre])
  (:import (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariConfig HikariDataSource)
           (java.util Properties)))

(defn- create-pool
  "Create a HikariDataSource for connection pooling from a properties filename."
  [pool-properties]
  (try
    (let [^Properties props (Properties.)]
      (doseq [[k v] pool-properties]
        (when (and k v)
          (.setProperty props k v)))
      (let [^HikariConfig config (HikariConfig. props)]
        (HikariDataSource. config)))
    (catch Exception e
      (timbre/error "Unable to create Hikari Datasource: " (.getMessage e)))))

(defn stop-connection-pools! [connection-pools]
  (doseq [[k ^HikariDataSource p] connection-pools]
    (timbre/info "Shutting down pool " k)
    (.close p)))

(defn migrate! [config connection-pools]
  (let [database-map (some-> config ::databases)]
    (doseq [[dbkey dbconfig] database-map]
      (let [{:keys [auto-migrate? migrations]} dbconfig
            ^HikariDataSource pool (get connection-pools dbkey)
            db                     {:datasource pool}]
        (if pool
          (do
            (timbre/info (str "Processing migrations for " dbkey))
            (when auto-migrate?
              (let [flyway (Flyway.)]
                (timbre/info "Migration location is set to: " migrations)
                (.setLocations flyway (into-array String migrations))
                (.setDataSource flyway pool)
                (.setBaselineOnMigrate flyway true)
                (.migrate flyway))))
          (timbre/error (str "No pool for " dbkey ". Skipping migrations.")))))))

(defn create-connection-pools! [config]
  (let [result (enc/if-let [databases (get config ::databases)
                            pools     (reduce (fn [pools [dbkey dbconfig]]
                                                (timbre/info (str "Creating connection pool for " dbkey))
                                                (assoc pools dbkey (create-pool (:hikaricp-config dbconfig)))) {} databases)]
                 (do
                   (try
                     (migrate! config pools)
                     (catch Throwable t
                       (timbre/error "DATABASE STARTUP FAILED: " t)
                       (stop-connection-pools! pools)
                       (throw t)))
                   pools)
                 (do
                   (timbre/error "SQL Database configuration missing/incorrect.")
                   (throw (ex-info "SQL Configuration failed." {}))))]
    result))

(defn next-id*
  [db schema table]
  (assert (s/valid? ::schema schema) "Next-id requires a valid schema.")
  (let [pk      (get-in schema [::pks table] :id)
        seqname (str (name table) "_" (name pk) "_seq")]
    (jdbc/query db [(str "SELECT nextval('" seqname "') AS \"id\"")]
      {:result-set-fn first
       :row-fn        :id})))

(defn next-id
  "Get the next generated ID for the given table.

  NOTE: IF you specify the Java System Property `dev`, then this function will assume you are writing tests and will
  allocate extra IDs in order to prevent assertions on your generated IDs across
  tables from giving false positives (since all tables will start from ID 1). It does this by throwing away a
  random number of IDs, so that IDs across tables are less likely to be identical when an equal number of rows
  are inserted."
  [db schema table-kw]
  (let [n (rand-int 20)]
    (when (System/getProperty "dev")
      (doseq [r (range n)]
        (next-id* db schema table-kw)))
    (next-id* db schema table-kw)))

(defn seed-row
  "Generate an instruction to insert a seed row for a table, which can contain keyword placeholders for IDs. It is
   recommended you namespace your generated IDs into `id` so that substitution during seeding doesn't cause surprises.
   For example:

  ```
  (seed-row :account {:id :id/joe ...})
  ```

  If the generated IDs appear in a PK location, they will be generated (must be unique per seed set). If they
  are in a value column, then the current generated value (which must have already been seeded) will be used.

  See also `seed-update` for resolving circular references.
  "
  [table value]
  (with-meta value {:table table}))

(defn seed-update
  "Generates an instruction to update a seed row (in the same seed set) that already appeared. This may be necessary if your database has
  referential loops.

  ```
  (seed-row :account {:id :id/joe ...})
  (seed-row :account {:id :id/sam ...})
  (seed-update :account :id/joe {:last_edited_by :id/sam })
  ```

  `table` should be a keyword form of the table in your database.
  `id` can be a real ID or a generated ID placeholder keyword (recommended: namespace it with `id`).
  `value` is a map of col/value pairs to update on the row.
  "
  [table id value]
  (with-meta value {:update id :table table}))

(defn join-key
  "Returns the key in a join. E.g. for {:k [...]} it returns :k"
  [join] (ffirst join))
(defn join-query
  "Returns the subquery of a join. E.g. for {:k [:data]} it returns [:data]."
  [join] (-> join first second))

(defn pk-column
  "Returns the SQL column for a given table's primary key"
  [schema table]
  (get-in schema [::pks table] :id))

(defn id-prop
  "Returns the SQL-centric property for the PK in a result set map (before conversion back to Om)"
  [schema table]
  (keyword (name table) (name (pk-column schema table))))

(defn seed!
  "Seed the given seed-row and seed-update items into the given database. Returns a map whose values will be the
  keyword placeholders for generated PK ids, and whose values are the real numeric generated ID:

  ```
  (let [{:keys [id/sam id/joe]} (seed! db schema [(seed-row :account {:id :id/joe ...})
                                                  (seed-row :account {:id :id/sam ...})]
    ...)
  ```
  "
  [db schema rows]
  (assert (s/valid? ::schema schema) "Schema is not valid")
  (let [tempid-map (reduce (fn [kws r]
                             (let [{:keys [update table]} (meta r)
                                   id (get r (pk-column schema table))]
                               (assert (or update id) "Expected an update or the row to contain the primary key")
                               (cond
                                 update kws
                                 (keyword? id) (assoc kws id (next-id db schema table))
                                 :else kws
                                 ))) {} rows)
        remap-row  (fn [row] (clojure.walk/postwalk (fn [e] (if (keyword? e) (get tempid-map e e) e)) row))]
    (doseq [row rows]
      (let [{:keys [update table]} (meta row)
            real-row (remap-row row)
            pk       (pk-column schema table)
            pk-val   (if update
                       (get tempid-map update update)
                       (get row pk))]
        (if update
          (do
            (timbre/debug "updating " row "at" pk pk-val)
            (jdbc/update! db table real-row [(str (name pk) " = ?") pk-val]))
          (do
            (timbre/debug "inserting " real-row)
            (jdbc/insert! db table real-row)))))
    tempid-map))

(def type-map
  {:string   "TEXT"
   :password "TEXT"
   :int      "INTEGER"
   :long     "BIGINT"
   :money    "decimal(20,2)"
   :inst     "BIGINT"
   :keyword  "TEXT"
   :symbol   "TEXT"
   :uuid     "UUID"})

(defn attr->table-name [{::keys [table]}] table)

(defn attr->column-name [{::attr/keys [qualified-key]}]
  (some-> qualified-key
    name
    csk/->snake_case))

(defn attr->sql [{::attr/keys [type identity?]
                  :as         attr}]
  (enc/when-let [table      (attr->table-name attr)
                 col        (attr->column-name attr)
                 typ        (get type-map type)
                 index-name (str table "_" col "_idx")]
    (str
      (format "CREATE TABLE IF NOT EXISTS %s ();\n" table)
      (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;\n" table col typ)
      (when identity?
        (format "CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(%s);\n" index-name table col)))))

(defn automatic-schema
  "Returns SQL schema for all attributes that support it."
  [attributes]
  (str
    "BEGIN;\n"
    (str/join "\n" (map attr->sql attributes))
    "COMMIT;\n"))

