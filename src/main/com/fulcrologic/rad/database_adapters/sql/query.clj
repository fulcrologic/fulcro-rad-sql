(ns com.fulcrologic.rad.database-adapters.sql.query
  "This namespace provides query builders for various concerns of
  a SQL database in a RAD application. The main concerns are:

  - Fetch queries (with joins) coming from resolvers
  - Building custom queries based of off RAD attributes
  - Persisting data based off submitted form deltas"
  (:require
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.sql :as rsql]
    [com.fulcrologic.rad.database-adapters.sql.schema :refer [column-name table-name]]
    [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
    [clojure.string :as str]
    [edn-query-language.core :as eql]
    [next.jdbc.sql :as jdbc.sql]
    [next.jdbc.sql.builder :as jdbc.builder]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [com.fulcrologic.guardrails.core :refer [>defn => | ?]]
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [next.jdbc.sql :as sql]
    [next.jdbc.result-set :as rs]))

(defn q [v]
  (if (int? v)
    v
    (str "'" v "'")))

(>defn to-many-join-column-query
  [{::attr/keys [key->attribute] :as env} {::attr/keys [target cardinality identities qualified-key] :as attr} ids]
  [any? ::attr/attribute coll? => (? (s/tuple string? ::attr/attributes))]
  (when (= :many cardinality)
    (do
      (when (not= 1 (count identities))
        (throw (ex-info "Reference column must have exactly 1 ::attr/identities entry." {:k qualified-key})))
      (enc/if-let [reverse-target-attr (key->attribute (first identities)) ; :account/id
                   target-attr         (key->attribute target) ; :address/id
                   rev-target-table    (table-name key->attribute reverse-target-attr) ; account
                   rev-target-column   (column-name reverse-target-attr) ; id
                   origin-table        (table-name key->attribute attr) ; account
                   origin-column       (column-name attr)   ; addresses
                   table               (table-name key->attribute target-attr) ; address
                   target-id-column    (column-name target-attr) ; id
                   ;; account_addresses_account_id
                   column              (or
                                         (::rsql/column-name attr)
                                         (str/join "_" [origin-table origin-column rev-target-table rev-target-column]))
                   id-list             (str/join "," (map q ids))]
        [(format "SELECT %1$s.%2$s AS c0, array_agg(%3$s.%4$s) AS c1 FROM %1$s LEFT JOIN %3$s ON %1$s.%2$s = %3$s.%5$s WHERE %1$s.%2$s IN (%6$s) GROUP BY %1$s.%2$s"
           rev-target-table rev-target-column table target-id-column column id-list)
         [reverse-target-attr attr]]
        (throw (ex-info "Cannot create to-many reference column." {:k qualified-key}))))))

(>defn base-property-query
  [env {id-key ::attr/qualified-key :as id-attr} attrs ids]
  [any? ::attr/attribute ::attr/attributes coll? => (s/tuple string? ::attr/attributes)]
  (let [attrs       (filter #(or
                               (not= :ref (::attr/type %))
                               (not= :many (::attr/cardinality %))) attrs)
        table       (table-name id-attr)
        id-column   (column-name id-attr)
        table-attrs (into [id-attr]
                      (filter #(contains? (::attr/identities %) id-key)) attrs)
        columns     (str/join "," (map-indexed (fn [idx a] (str table "." (column-name a) " AS c" idx)) table-attrs))
        id-list     (str/join "," (map q ids))]
    [(format "SELECT %s FROM %s WHERE %s IN (%s)" columns table id-column id-list) table-attrs]))

;; TASK: basic type coercion...probably at least need enum handling
(defn coerce
  ""
  [value type]
  (case type
    :enum (when value (read-string value))
    value))

(defn- interpret-result [value {::attr/keys [cardinality type target]}]
  (cond
    (and (= :ref type) (not= :many cardinality))
    {target value}

    (= :ref type)
    (mapv (fn [id] {target id}) value)

    :else (coerce value type)))

(defn- convert-row [row attrs]
  (when-not (contains? row :C0)
    (log/error "Row is missing :Cn entries!" row))
  (:result
    (reduce
      (fn [{:keys [index result]} {::attr/keys [qualified-key] :as attr}]
        (let [value (interpret-result (get row (keyword (str "C" index))) attr)]
          {:index  (inc index)
           :result (assoc result qualified-key value)}))
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

(>defn eql-query!
  [{::attr/keys [key->attribute]
    ::rsql/keys [connection-pools]
    :as         env} id-attribute eql-query resolver-input]
  [any? ::attr/attribute ::eql/query coll? => (? coll?)]
  (let [schema            (::attr/schema id-attribute)
        datasource        (or (get connection-pools schema) (throw (ex-info "Data source missing for schema" {:schema schema})))
        id-key            (::attr/qualified-key id-attribute)
        attrs-of-interest (eql->attrs env schema eql-query)
        to-many-joins     (filterv #(and (= :many (::attr/cardinality %))
                                      (= :ref (::attr/type %))) attrs-of-interest)
        ids               (if (map? resolver-input)
                            [(or (get resolver-input id-key) (log/error "Resolver input missing ID" {:input resolver-input}))]
                            (mapv #(or (get % id-key)
                                     (log/error ex-info "Resolver input missing ID" {:input resolver-input})) resolver-input))
        [base-query base-attributes] (base-property-query env id-attribute attrs-of-interest ids)
        joins-to-run      (mapv #(to-many-join-column-query env % ids) to-many-joins)
        one?              (map? resolver-input)]
    (when (seq ids)
      (let [rows                  (sql/query datasource [base-query] {:builder-fn rs/as-unqualified-maps})
            base-result-map-by-id (enc/keys-by id-key (sql-results->edn-results rows base-attributes))
            results-by-id         (reduce
                                    (fn [result [join-query join-attributes]]
                                      (let [join-rows         (sql/query datasource [join-query] {:builder-fn rs/as-unqualified-maps})
                                            join-eql-results  (sql-results->edn-results join-rows join-attributes)
                                            join-result-by-id (enc/keys-by id-key join-eql-results)]
                                        (deep-merge result join-result-by-id)))
                                    base-result-map-by-id
                                    joins-to-run)]
        (if one?
          (first (vals results-by-id))
          (vec (vals results-by-id)))))))
