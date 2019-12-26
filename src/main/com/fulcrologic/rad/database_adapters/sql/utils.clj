(ns com.fulcrologic.rad.database-adapters.sql.utils
  (:require
    [camel-snake-kebab.core                    :as csk]
    [clojure.string                            :as str]
    [com.fulcrologic.rad.attributes            :as attr]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]))


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
