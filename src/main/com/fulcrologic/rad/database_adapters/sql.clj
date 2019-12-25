(ns com.fulcrologic.rad.database-adapters.sql
  (:require
    [com.fulcrologic.rad.database-adapters.sql.connection :as sql.conn]
    [com.fulcrologic.rad.database-adapters.sql.migration  :as sql.migration]
    [com.fulcrologic.rad.database-adapters.sql.query      :as sql.query]
    [com.fulcrologic.rad.database-adapters.sql.resolvers  :as sql.resolvers]))

;; Connection
(def stop-connection-pools!   #'sql.conn/stop-connection-pools!)
(def create-connection-pools! #'sql.conn/create-connection-pools!)
(def migrate!                 #'sql.migration/migrate!)

;; Queries and Resolvers
(def query              #'sql.query/query)
(def where-attributes   #'sql.query/where-attributes)
(def generate-resolvers #'sql.resolvers/generate-resolvers)
(def save-form!         #'sql.resolvers/save-form!)
