(ns com.fulcrologic.rad.database-adapters.sql.utils
  (:require
    [camel-snake-kebab.core         :as csk]
    [clojure.string                 :as str]
    [com.fulcrologic.rad.attributes :as attr]))


(defmacro alias!
  "Trick to use top level API keywords as params, but have
  implementations in seperate namespaces.
  Don't abuse this, should only be used to split up big top level API
  namespaces."
  [alias namespace-sym]
  `(do
     (create-ns ~namespace-sym)
     (alias ~alias ~namespace-sym)))

(alias! 'rad.sql 'com.fulcrologic.rad.database-adapters.sql)


(defn attr->table-names [{::rad.sql/keys [tables]}]
  tables)


(defn attr->table-name
  "Helpful but temporary, until we cleanup up the multi db / table
  story"
  [{::rad.sql/keys [tables]}]
  (first tables))


(defn attr->column-name [{::attr/keys [qualified-key]
                          ::rad.sql/keys      [column-name]}]
  (or
    column-name
    (some-> qualified-key name csk/->snake_case)))
