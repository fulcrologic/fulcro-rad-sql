(ns com.fulcrologic.rad.database-adapters.sql.result-set
  "This namespace provides some tools to work with jdbc result sets"
  (:require [next.jdbc.result-set :as jdbc.rs])
  (:import (java.sql ResultSet ResultSetMetaData)))

(defn- get-column-names [^ResultSetMetaData meta opts]
  (assert (:key-fn opts) ":key-fn is required")
  (mapv (fn [^Integer i]
          ((:key-fn opts)
           (.getTableName meta i)
           (.getColumnLabel meta i)))
    (range 1 (inc (.getColumnCount meta)))))

(defn as-qualified-maps
  "A result set builder, but instead of using a `:label-fn` and
  `:qualifier-fn`, it requires a `key-fn` in the sql-opts. `key-fn`
  takes a table name and column name straight from the result-set
  meta, and returns the fully qualified key."
  [^ResultSet rs opts]
  (let [rsmeta (.getMetaData rs)
        cols   (get-column-names rsmeta opts)]
    (jdbc.rs/->MapResultSetBuilder rs rsmeta cols)))
