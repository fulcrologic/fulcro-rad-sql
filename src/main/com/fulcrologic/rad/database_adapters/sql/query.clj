(ns com.fulcrologic.rad.database-adapters.sql.query
  "This namespace provides query builders for various concerns of
  a SQL database in a RAD application. The main concerns are:

  - Fetch queries (with joins) coming from resolvers
  - Building custom queries based of off RAD attributes
  - Persisting data based off submitted form deltas"
  (:require
    [com.fulcrologic.rad.attributes                       :as rad.attr]
    [com.fulcrologic.rad.database-adapters.sql            :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.result-set :as sql.rs]
    [com.fulcrologic.rad.database-adapters.sql.schema     :as sql.schema]
    [clojure.string                                       :as str]
    [edn-query-language.core                              :as eql]
    [next.jdbc.sql                                        :as jdbc.sql]
    [next.jdbc.sql.builder                                :as jdbc.builder]
    [taoensso.encore                                      :as enc]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Entity / EQL query

(defn query->plan
  "Given an EQL query, plans a sql query that would fetch the entities
  and their joins"
  [query {:keys [::rad.attr/k->attr ::rad.attr/id-attribute]}]
  (let [{:keys [prop join]} (group-by :type (:children (eql/query->ast query)))
        table               (::rad.sql/table id-attribute)
        ->fields
        (fn [{:keys [nodes id-attr parent-attr]}]
          (for [node nodes
                :let [k (:dispatch-key node)
                      attr (k->attr k)]]
            {::rad.attr/qualified-key k
             ::rad.attr/cardinality   (::rad.attr/cardinality parent-attr)
             ::rad.sql/table          (::rad.sql/table id-attr)
             ::rad.sql/column         (sql.schema/attr->column-name attr)
             ::parent-key             (::rad.attr/qualified-key parent-attr)}))]
    {::fields (apply concat
                (->fields {:nodes prop :id-attr id-attribute})
                (for [node join]
                  (enc/if-let [parent-attr (-> node :dispatch-key k->attr)
                               target-attr (-> parent-attr ::rad.attr/target k->attr)]
                    (->fields {:nodes       (:children node)
                               :id-attr     target-attr
                               :parent-attr parent-attr})
                    (throw (ex-info "Invalid target for join"
                             {:key (:dispatch-key node)})))))
     ::from table
     ::joins (for [node join
                   :let [attr (k->attr (:dispatch-key node))]]
               [(::rad.sql/join attr)
                [table (sql.schema/attr->column-name id-attribute)]])
     ::group (when (seq join)
               [[table (sql.schema/attr->column-name id-attribute)]])}))


(defn plan->sql
  "Given a query plan, return the sql statement that matches the plan"
  [{::keys [fields from joins group]}]
  (let [field-name (partial format "%s.\"%s\"")
        fields-sql (str/join ", " (for [{::rad.sql/keys [table column]
                                         ::keys [parent-key]} fields]
                                    (if parent-key
                                      (format "array_agg(%s)" (field-name table column))
                                      (field-name table column))))
        join-sql   (str/join " "
                     (for [[[ltable lcolumn] [rtable rcolumn]] joins]
                       (str "LEFT JOIN " ltable " ON "
                         (field-name ltable lcolumn) " = "
                         (field-name rtable rcolumn))))
        group-sql  (when (seq group)
                     (str "GROUP BY "
                       (str/join ", "
                         (for [[table column] group]
                           (field-name table column)))))]
    (format "SELECT %s FROM %s %s %s" fields-sql from join-sql group-sql)))


(defn- unnest
  "A helper function to unnest aggregated values in a to many ref.
  Example:
  ``` clojure
  {:account/id [1 2] :account/name [\"Name1\" \"name2\"]}
  => [{:account/id 1 :account/name \"Name1\"}
      {:account/id 2 :account/name \"Name2\"}]
  ``` "
  [record ks]
  (apply map
    (fn [& vals] (zipmap ks vals))
    (map record ks)))


(defn parse-executed-plan
  "After a plan is executed, unnest any aggregated nested fields"
  [{::keys [fields]} result]
  (let [nested-fields (group-by ::parent-key (filter ::parent-key fields))
        clean-left-join-nils (fn [children]
                               (if (and (= (count children) 1)
                                     (every? nil? (vals (first children))))
                                 (empty children) children))
        first-if-one (fn [fields children]
                       (if (= :many (::rad.attr/cardinality (first fields)))
                         children (first children)))]
    (if (seq nested-fields)
      (for [record result]
        (reduce-kv
          (fn [record parent-key fields]
            (assoc (apply dissoc record (map ::rad.attr/qualified-key fields))
              parent-key (->> (map ::rad.attr/qualified-key fields)
                           (unnest record)
                           (clean-left-join-nils)
                           (first-if-one fields))))
          record nested-fields))
      result)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public


(defn query
  "Wraps next.jbdc's query, but will return fully qualified keywords
  for any matching attributes found in `::attr/attributes` in the
  options map."
  [db stmt opts]
  (jdbc.sql/query (:datasource db)
    stmt
    (merge {:builder-fn sql.rs/as-qualified-maps
            :key-fn (let [idx (sql.schema/attrs->sql-col-index
                                (::rad.attr/attributes opts))]
                      (fn [table column]
                        (get idx [table column]
                          (keyword table column))))}
      opts)))


(defn where-attributes
  "Generates a query using next.jdbc's builders, and executes it. Any
  columns in the result set will be efficiently namespaced according
  to the attributes in `::attr/attributes` option."
  [db table where opts]
  (let [where-clause (enc/map-keys sql.schema/attr->column-name where)]
    (query db (jdbc.builder/for-query table where-clause opts) opts)))


(defn eql-query
  "Generates and executes a query based off off an EQL query by using
  the `::rad.attr/k->attr`. There is no restriction on the number
  of joined tables, but the maximum depth of these queries are 1."
  [db query env]
  (let [plan (query->plan query env)
        sql  (plan->sql plan)
        opts (assoc env
               :builder-fn sql.rs/as-maps-with-keys
               :keys (map ::rad.attr/qualified-key (::fields plan)))
        result (jdbc.sql/query (:datasource db) [sql] opts)]
    (with-meta
      (parse-executed-plan plan result)
      {::sql sql
       ::plan plan})))
