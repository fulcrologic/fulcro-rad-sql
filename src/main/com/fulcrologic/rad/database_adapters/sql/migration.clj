(ns com.fulcrologic.rad.database-adapters.sql.migration
  (:require
    [next.jdbc                                        :as jdbc]
    [clojure.string                                   :as str]
    [com.fulcrologic.rad.attributes                   :as attr]
    [taoensso.encore                                  :as enc]
    [taoensso.timbre                                  :as log]
    [com.fulcrologic.rad.database-adapters.sql        :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema])
  (:import (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariDataSource)))

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

;; TASK: Review this generation, and use it in demo instead of flyway?
(defn attr->sql [schema-name {::attr/keys [type identity?]
                              ::rad.sql/keys      [schema]
                              :as         attr}]
  (when (= schema schema-name)
    (enc/if-let [tables (seq (sql.schema/attr->table-names attr))
                 col    (sql.schema/attr->column-name attr)
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

(defn migrate! [config all-attributes connection-pools]
  (let [database-map (some-> config ::rad.sql/databases)]
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
              (jdbc/execute! (:datasource db) (automatic-schema schema all-attributes))))
          (log/error (str "No pool for " dbkey ". Skipping migrations.")))))))
