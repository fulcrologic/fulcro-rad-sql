(ns com.fulcrologic.rad.database-adapters.sql.schema
  "This namespace provides any conversion necessary between RAD
  attributes and their corresponding table / columns in the database."
  (:require
    [camel-snake-kebab.core                    :as csk]
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


(defn attrs->sql-col-index
  "Takes a list of rad attributes and returns an index of the form
  `{[table column] :qualified/keyword}`"
  [attributes]
  (into {}
    (for [attr attributes]
      [[(attr->table-name attr) (attr->column-name attr)]
       (::attr/qualified-key attr)])))
