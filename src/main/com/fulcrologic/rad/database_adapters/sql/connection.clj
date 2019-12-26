(ns com.fulcrologic.rad.database-adapters.sql.connection
  (:require
    [com.fulcrologic.rad.database-adapters.sql.migration :as sql.migration]
    [com.fulcrologic.rad.database-adapters.sql           :as rad.sql]
    [taoensso.encore                                     :as enc]
    [taoensso.timbre                                     :as log])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)
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
      (log/error "Unable to create Hikari Datasource: " (.getMessage e)))))


(defn stop-connection-pools! [connection-pools]
  (doseq [[k ^HikariDataSource p] connection-pools]
    (log/info "Shutting down pool " k)
    (.close p)))


(defn create-connection-pools! [config all-attributes]
  (enc/if-let [databases (get config ::rad.sql/databases)
               pools (reduce
                       (fn [pools [dbkey dbconfig]]
                         (log/info (str "Creating connection pool for " dbkey))
                         (assoc pools
                           dbkey (create-pool (:hikaricp/config dbconfig))))
                       {} databases)]
    (try
      (sql.migration/migrate! config all-attributes pools)
      pools
      (catch Throwable t
        (log/error "DATABASE STARTUP FAILED: " t)
        (stop-connection-pools! pools)
        (throw t)))
    (do
      (log/error "SQL Database configuration missing/incorrect.")
      (throw (ex-info "SQL Configuration failed." {})))))
