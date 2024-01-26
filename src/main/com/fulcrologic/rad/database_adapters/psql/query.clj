(ns com.fulcrologic.rad.database-adapters.psql.query
  "This namespace provides query builders for various concerns of
  a SQL database in a RAD application. The main concerns are:

  - Fetch queries (with joins) coming from resolvers
  - Building custom queries based of off RAD attributes
  - Persisting data based off submitted form deltas"
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
    [com.fulcrologic.guardrails.core :refer [=> >defn ?]]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.attributes-options :as ao]
    [com.fulcrologic.rad.database-adapters.psql.schema :refer [column-name table-name]]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [edn-query-language.core :as eql]
    [next.jdbc.result-set :as rs]
    [next.jdbc.sql]
    [next.jdbc.sql :as sql]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log])
  (:import (java.sql ResultSet ResultSetMetaData Types)))

(defn q [v]
  (cond
    (nil? v) nil
    (int? v) v
    (boolean? v) v
    :else (str "'" v "'")))

(>defn to-many-join-column-query
  [{::attr/keys [key->attribute] :as env} {::attr/keys [target cardinality identities qualified-key] :as attr} ids]
  [any? ::attr/attribute coll? => (? (s/tuple string? ::attr/attributes))]
  (log/spy :debug qualified-key)
  (when (= :many (log/spy :debug cardinality))
    (do
      (when (not= 1 (count identities))
        (throw (ex-info "Reference column must have exactly 1 ::attr/identities entry." {:k qualified-key})))
      (enc/if-let [reverse-target-attr (key->attribute (first identities)) ; :account/id
                   target-attr         (key->attribute target) ; :address/id
                   rev-target-table    (table-name key->attribute reverse-target-attr) ; account
                   rev-target-column   (column-name reverse-target-attr) ; id
                   column              (column-name key->attribute attr) ; account_addresses_account_id
                   table               (table-name key->attribute target-attr) ; address
                   target-id-column    (column-name target-attr) ; id
                   id-list             (str/join "," (map q ids))]
        [(format "SELECT %1$s.%2$s AS c0, array_agg(%3$s.%4$s) AS c1 FROM %1$s LEFT JOIN %3$s ON %1$s.%2$s = %3$s.%5$s WHERE %1$s.%2$s IN (%6$s) GROUP BY %1$s.%2$s"
           rev-target-table rev-target-column table target-id-column column id-list)
         [reverse-target-attr attr]]
        (throw (ex-info "Cannot create to-many reference column." {:k qualified-key}))))))

(>defn base-property-query
  "Generates a query that will get everything that isn't a ref"
  [env {id-key ::attr/qualified-key :as id-attr} attrs]
  [any? ::attr/attribute ::attr/attributes => (s/tuple string? ::attr/attributes)]
  (let [attrs       (filter #(or
                               (not= :ref (::attr/type %))
                               (not= :many (::attr/cardinality %))) attrs)
        table       (table-name id-attr)
        id-column   (column-name id-attr)
        table-attrs (into [id-attr]
                      (filter #(contains? (::attr/identities %) id-key)) attrs)
        columns     (str/join "," (map-indexed (fn [idx a] (str table "." (column-name a) " AS c" idx)) table-attrs))]
    [(format "SELECT %s FROM %s WHERE %s =ANY (?)" columns table id-column) table-attrs]))

(defn sql->form-value [{::attr/keys    [type]
                        ::rad.sql/keys [sql->form-value]} sql-value]
  (cond
    sql->form-value (sql->form-value sql-value)
    (and (= type :enum) (string? sql-value) (str/starts-with? sql-value ":")) (read-string (log/spy :debug sql-value))
    :else sql-value))

(defn- interpret-result [value {::attr/keys [cardinality type target] :as attr}]
  (when (not (nil? value))
    (cond
      (and (= :ref type) (not= :many cardinality))
      (log/spy :info {target value})

      (= :ref type)
      (vec (keep (fn [id] (when id {target id})) value))

      :else (sql->form-value attr value))))

(defn- convert-row [row attrs]
  (when-not (contains? row :c0)
    (log/error "Row is missing :cn entries!" row))
  (:result
    (reduce
      (fn [{:keys [index result]} {::attr/keys [qualified-key] :as attr}]
        (let [value (interpret-result (get row (keyword (str "c" index))) attr)]
          {:index  (inc index)
           :result (if (nil? value)
                     result
                     (assoc result qualified-key value))}))
      {:index  0
       :result {}}
      attrs)))

(>defn sql-results->edn-results
  [rows attrs]
  [(s/coll-of map?) ::attr/attributes => (s/coll-of map?)]
  (mapv #(convert-row % attrs) rows))

(>defn eql->attrs
  "Find the attributes for the element of the (top-level) EQL query that exist on the given schema.
  Returns a sequence of attributes."
  [{::attr/keys [key->attribute] :as env} schema eql]
  [any? keyword? ::eql/query => ::attr/attributes]
  (let [nodes (:children (eql/query->ast eql))]
    (into []
      (keep
        (fn [{:keys [dispatch-key]}]
          (let [{attr-schema ::attr/schema :as attr} (key->attribute dispatch-key)]
            (when (= attr-schema schema)
              attr))))
      nodes)))

(defn RAD-column-reader
  "An example column-reader that still uses `.getObject` but expands CLOB
  columns into strings."
  [^ResultSet rs ^ResultSetMetaData md ^Integer i]
  (let [col-type (.getColumnType md i)]
    (cond
      (= col-type Types/CLOB) (rs/clob->string (.getClob rs i))
      (#{Types/TIMESTAMP Types/TIMESTAMP_WITH_TIMEZONE} col-type) (.getTimestamp rs i)
      :else (.getObject rs i))))

;; Type coercion is handled by the row builder
(def row-builder (rs/as-maps-adapter rs/as-unqualified-lower-maps RAD-column-reader))

(>defn eql-query!
  [{::attr/keys    [key->attribute]
    ::rad.sql/keys [connection-pools]
    :as            env} id-attribute eql-query resolver-input]
  [any? ::attr/attribute ::eql/query coll? => (? coll?)]
  (let [schema            (::attr/schema id-attribute)
        datasource        (or (get connection-pools schema) (throw (ex-info "Data source missing for schema" {:schema schema})))
        id-key            (::attr/qualified-key id-attribute)
        _                 (assert (#{:int :long} (ao/type id-attribute)) "ID type it integer")
        attrs-of-interest (eql->attrs env schema eql-query)
        to-many-joins     (filterv #(and (= :many (::attr/cardinality %))
                                      (= :ref (::attr/type %))) attrs-of-interest)
        ids               (if (map? resolver-input)
                            (if-let [id (get resolver-input id-key)] [id] [])
                            (vec (keep #(get % id-key) resolver-input)))]
    (when (seq ids)
      (let [[base-query base-attributes] (base-property-query env id-attribute attrs-of-interest)
            joins-to-run          (mapv #(to-many-join-column-query env % ids) to-many-joins)
            one?                  (map? resolver-input)
            rows                  (log/spy :debug (sql/query datasource (log/spy :debug [base-query (int-array ids)]) {:builder-fn row-builder}))
            base-result-map-by-id (log/spy :debug (enc/keys-by id-key (log/spy :debug (sql-results->edn-results rows base-attributes))))
            results-by-id         (reduce
                                    (fn [result [join-query join-attributes]]
                                      (let [join-rows         (log/spy :debug (sql/query datasource [(log/spy :debug join-query)] {:builder-fn row-builder}))
                                            join-eql-results  (log/spy :debug (sql-results->edn-results join-rows join-attributes))
                                            join-result-by-id (log/spy :debug (enc/keys-by id-key join-eql-results))]
                                        (deep-merge result join-result-by-id)))
                                    base-result-map-by-id
                                    joins-to-run)]
        (if one?
          (first (vals results-by-id))
          (mapv results-by-id ids))))))
