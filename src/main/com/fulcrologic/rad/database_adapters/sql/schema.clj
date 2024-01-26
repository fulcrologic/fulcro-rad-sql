(ns com.fulcrologic.rad.database-adapters.sql.schema
  "This namespace provides any conversion necessary between RAD
  attributes and their corresponding table / columns in the database."
  (:require
    [camel-snake-kebab.core :as csk]
    [com.fulcrologic.guardrails.core :refer [>defn => |]]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [taoensso.timbre :as log]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [taoensso.encore :as enc]))

(>defn table-name
  "Get the table name for a given identity key"
  ([key->attribute {::attr/keys    [qualified-key identity? identities]
                    ::rad.sql/keys [table] :as attr}]
   [map? ::attr/attribute => string?]
   (if identity?
     (table-name attr)
     (let [identity (first identities)
           id-attr  (key->attribute identity)]
       (when-not (= 1 (count identities))
         (throw (ex-info "Cannot derive table name from ::attr/identities because there is more than one."
                  {:attr attr
                   :k    qualified-key})))
       (table-name id-attr))))
  ([{::attr/keys    [identity? qualified-key]
     ::rad.sql/keys [table]}]
   [::attr/attribute => string?]
   (when-not identity?
     (throw (ex-info "You must use an id-attribute with table-name" {:non-id-key qualified-key})))
   (or table (some-> qualified-key namespace csk/->snake_case))))

(defn attr->table-name
  "DEPRECATED. use `table-name` on an id attr. This one cannot be correct, since an attr can be on more than one tbl"
  ([k->attr {:keys [::attr/identities ::rad.sql/table]}]
   (or table (get-in k->attr [(first identities) ::rad.sql/table]))))

(defn column-name
  "Get the column name for the given attribute."
  ([k->attr {::attr/keys [identities cardinality type] :as attr}]
   (or
     (::rad.sql/column-name attr)
     (if (and (= :many cardinality) (= :ref type))
       (do
         (when-not (= 1 (count identities))
           (throw (ex-info "Cannot calculating column name that has multiple identities." {:attr attr})))
         (enc/if-let [reverse-target-attr (k->attr (first identities))
                      rev-target-table    (table-name k->attr reverse-target-attr)
                      rev-target-column   (column-name reverse-target-attr)
                      origin-table        (table-name k->attr attr)
                      origin-column       (some-> attr ::attr/qualified-key name csk/->snake_case)]
           ;; account_addresses_account_id
           (str/join "_" [origin-table origin-column rev-target-table rev-target-column])))
       (column-name attr))))
  ([{::attr/keys    [qualified-key cardinality type]
     ::rad.sql/keys [column-name]}]
   (when (and (= :many cardinality) (= :ref type))
     (throw (ex-info "Cannot calculate column name for to-many ref without k->attr." {:attr qualified-key})))
   (or
     column-name
     (some-> qualified-key name csk/->snake_case))))

(defn sequence-name [id-attribute]
  (str (table-name id-attribute) "_" (column-name id-attribute) "_seq"))

(def attr->column-name column-name)

(defn tables-and-columns
  "Return a sequence of [table-name column-name] that the given attribute appears at."
  [key->attribute {::attr/keys [identity? identities] :as attribute}]
  [(s/map-of qualified-keyword? ::attr/attribute) ::attr/attribute => (s/coll-of (s/tuple string? string?))]
  (let [col-name (column-name key->attribute attribute)]
    (if identity?
      [[(table-name attribute) col-name]]
      (mapv
        (fn [id-key]
          (let [id-attribute (key->attribute id-key)]
            [(table-name id-attribute) col-name]))
        identities))))

(defn attrs->sql-col-index
  "Takes a list of rad attributes and returns an index of the form
  `{[table column] :qualified/keyword}`"
  [k->attr]
  (into {}
    (mapcat
      (fn [attr] (map (fn [k] [k attr]) (tables-and-columns k->attr attr)))
      (vals k->attr))))
