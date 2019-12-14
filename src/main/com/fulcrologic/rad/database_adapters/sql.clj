(ns com.fulcrologic.rad.database-adapters.sql
  (:require
    [camel-snake-kebab.core :as csk]
    [clojure.java.jdbc :as jdbc]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.wsscode.pathom.connect :as pc]
    [com.fulcrologic.guardrails.core :refer [>defn =>]]
    [com.fulcrologic.rad.attributes :as attr]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [com.fulcrologic.rad.authorization :as auth]
    [edn-query-language.core :as eql])
  (:import (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariConfig HikariDataSource)
           (java.util Properties)))

(def type-map
  {:string   "TEXT"
   :password "TEXT"
   :boolean  "BOOLEAN"
   :int      "INTEGER"
   :long     "BIGINT"
   :money    "decimal(20,2)"
   :inst     "BIGINT"
   :keyword  "TEXT"
   :symbol   "TEXT"
   :uuid     "UUID"})

(defn attr->table-names [{::keys [tables]}]
  tables)

(defn attr->column-name [{::attr/keys [qualified-key]
                          ::keys      [column-name]}]
  (or
    column-name
    (some-> qualified-key name csk/->snake_case)))

(defn attr->sql [schema-name {::attr/keys [type identity?]
                              ::keys      [schema]
                              :as         attr}]
  (when (= schema schema-name)
    (enc/if-let [tables (seq (attr->table-names attr))
                 col    (attr->column-name attr)
                 typ    (get type-map type)]
      (str/join "\n"
        (for [table tables]
          (let [index-name (str table "_" col "_idx")]
            (str
              (format "CREATE TABLE IF NOT EXISTS %s ();\n" table)
              (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;\n"
                table col typ)
              (when identity?
                (format "CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(%s);\n"
                  index-name table col))))))
      (log/error "Correct schema for attribute, but generation failed: "
        (::attr/qualified-key attr)
        (when (nil? (get type-map type))
          (str " (No mapping for type " type ")"))))))

(defn automatic-schema
  "Returns SQL schema for all attributes that support it."
  [schema-name attributes]
  (let [statements (mapv (partial attr->sql schema-name) attributes)]
    (str
      "BEGIN;\n"
      (str/join "\n" statements)
      "COMMIT;\n")))

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
      (log/error "Unable to create Hikari Datasource: " (.getMessage e)))))

(defn stop-connection-pools! [connection-pools]
  (doseq [[k ^HikariDataSource p] connection-pools]
    (log/info "Shutting down pool " k)
    (.close p)))

(defn migrate! [config all-attributes connection-pools]
  (let [database-map (some-> config ::databases)]
    (doseq [[dbkey dbconfig] database-map]
      (let [{:sql/keys    [auto-create-missing? schema]
             :flyway/keys [migrate? migrations]} dbconfig
            ^HikariDataSource pool (get connection-pools dbkey)
            db                     {:datasource pool}]
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
            (do
              (log/info "Automatically trying to create SQL schema from attributes.")
              (jdbc/execute! db (automatic-schema schema all-attributes))))
          (log/error (str "No pool for " dbkey ". Skipping migrations.")))))))

(defn create-connection-pools! [config all-attributes]
  (enc/if-let [databases (get config ::databases)
               pools (reduce
                       (fn [pools [dbkey dbconfig]]
                         (log/info (str "Creating connection pool for " dbkey))
                         (assoc pools
                           dbkey (create-pool (:hikaricp/config dbconfig))))
                       {} databases)]
    (try
      (migrate! config all-attributes pools)
      pools
      (catch Throwable t
        (log/error "DATABASE STARTUP FAILED: " t)
        (stop-connection-pools! pools)
        (throw t)))
    (do
      (log/error "SQL Database configuration missing/incorrect.")
      (throw (ex-info "SQL Configuration failed." {})))))

(defn id->query-value [id-attr v]
  (let [t (::attr/type id-attr)]
    (case t
      (:text :uuid) (str "'" v "'")
      v)))

(defn column-names [attributes query]
  (let [desired-keys       (->> query
                             (eql/query->ast)
                             (:children)
                             (map :dispatch-key)
                             (set))
        desired-attributes (filterv
                             #(contains? desired-keys (::attr/qualified-key %))
                             attributes)]
    (mapv attr->column-name desired-attributes)))

(defn entity-query [{::keys [schema attributes id-attribute] :as env} input]
  (let [one? (not (sequential? input))]
    (enc/if-let [db               (get-in env [::databases schema])
                 id-key           (::attr/qualified-key id-attribute)
                 table            (-> id-attribute ::tables first)
                 query            (or
                                    (get env :com.wsscode.pathom.core/parent-query)
                                    (get env ::default-query))
                 to-v             (partial id->query-value id-attribute)
                 ids              (if one?
                                    [(to-v (get input id-key))]
                                    (into [] (keep #(some-> %
                                                      (get id-key)
                                                      to-v) input)))
                 column-name->key (into {}
                                    (for [attr attributes]
                                      [(attr->column-name attr)
                                       (::attr/qualified-key attr)]))
                 sql              (str
                                    "SELECT " (str/join ", "
                                                (column-names attributes query))
                                    " FROM " table
                                    " WHERE " (attr->column-name id-attribute)
                                    " IN (" (str/join "," ids) ")")]
      (do
        (log/info "Running" sql "on entities with " id-key ":" ids)
        (let [result (jdbc/query db [sql]
                       {:keywordize  false
                        :identifiers column-name->key})]
          (if one?
            (first result)
            result)))
      (log/info "Unable to complete query."))))

;; TODO: This is nearly identical to the Datomic one...helper function
;; would be nice
(defn id-resolver [id-attr attributes]
  (enc/if-let [id-key        (::attr/qualified-key id-attr)
               outputs       (attr/attributes->eql attributes)
               schema        (::schema id-attr)]
    {::pc/sym     (symbol
                    (str (namespace id-key))
                    (str (name id-key) "-resolver"))
     ::pc/output  outputs
     ::pc/batch?  true
     ::pc/resolve (fn [env input] (->>
                                    (entity-query
                                      (assoc env
                                        ::attributes attributes
                                        ::id-attribute id-attr
                                        ::schema schema
                                        ::attr/qualified-key id-key
                                        ::default-query outputs)
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
  (let [attributes          (filter #(= schema (::schema %)) attributes)
        table->id-attribute (reduce
                              (fn [m {::attr/keys [identity?]
                                      ::keys      [tables] :as attr}]
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
                                  (for [table (get attribute ::tables)
                                        :let [id-attr (table->id-attribute table)]]
                                    (assoc attribute ::k id-attr))))
                              (group-by ::k))]
    (reduce-kv
      (fn [resolvers id-attr attributes]
        (conj resolvers (id-resolver id-attr attributes)))
      [] id-attr->attributes)))
