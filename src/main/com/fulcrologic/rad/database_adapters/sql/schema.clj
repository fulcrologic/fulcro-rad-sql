(ns com.fulcrologic.rad.database-adapters.sql.schema
  "This namespace provides any conversion necessary between RAD
  attributes and their corresponding table / columns in the database."
  (:require
    [camel-snake-kebab.core                    :as csk]
    [com.fulcrologic.rad.attributes            :as rad.attr]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]))


(defn attr->table-names
  "BROKEN: Fix migrations"
  [{::rad.sql/keys [tables]}]
  tables)


(defn attr->table-name
  ([_] ;; Broken for now
   )
  ([k->attr {:keys [::rad.sql/entity-ids ::rad.sql/table]}]
   (or table (get-in k->attr [(first entity-ids) ::rad.sql/table]))))


(defn attr->column-name [{::rad.attr/keys [qualified-key]
                          ::rad.sql/keys  [column-name]}]
  (or
    column-name
    (some-> qualified-key name csk/->snake_case)))


(defn attrs->sql-col-index
  "Takes a list of rad attributes and returns an index of the form
  `{[table column] :qualified/keyword}`"
  [k->attr]
  (into {}
    (for [[_ attr] k->attr]
      [[(attr->table-name k->attr attr) (attr->column-name attr)]
       (::rad.attr/qualified-key attr)])))
