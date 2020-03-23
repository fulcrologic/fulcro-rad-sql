(ns com.fulcrologic.rad.database-adapters.sql.query
  "This namespace provides query builders for various concerns of
  a SQL database in a RAD application. The main concerns are:

  - Fetch queries (with joins) coming from resolvers
  - Building custom queries based of off RAD attributes
  - Persisting data based off submitted form deltas"
  (:require
    [com.fulcrologic.rad.attributes :as rad.attr]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.result-set :as sql.rs]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
    [clojure.string :as str]
    [edn-query-language.core :as eql]
    [next.jdbc.sql :as jdbc.sql]
    [next.jdbc.sql.builder :as jdbc.builder]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [clojure.set :as set]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Entity / EQL query

(defn query->plan
  "Given an EQL query, plans a sql query that would fetch the entities
  and their joins"
  [query where {:keys [::rad.attr/k->attr ::rad.sql/id-attribute]}]
  (let [{:keys [prop join]} (group-by :type (:children (eql/query->ast query)))
        table (::rad.sql/table id-attribute)
        ->fields
              (fn [{:keys [nodes id-attr parent-attr]}]
                (for [node nodes
                      :let [k    (:dispatch-key node)
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
     ::from   table
     ::joins  (for [node join
                    :let [attr (k->attr (:dispatch-key node))]]
                [(::rad.sql/join attr)
                 [table (sql.schema/attr->column-name id-attribute)]])
     ::where  (enc/map-keys
                ;; TODO table may be incorrect
                #(format "%s.\"%s\"" table
                   (sql.schema/attr->column-name (k->attr %)))
                where)
     ::group  (when (seq join)
                [[table (sql.schema/attr->column-name id-attribute)]])}))


(defn plan->sql
  "Given a query plan, return the sql statement that matches the plan"
  [{::keys [fields from joins where group]}]
  (let [field-name (partial format "%s.\"%s\"")
        fields-sql (str/join ", " (for [{::rad.sql/keys [table column]
                                         ::keys         [parent-key]} fields]
                                    (if parent-key
                                      (format "array_agg(%s)"
                                        (field-name table column))
                                      (field-name table column))))
        join-sql   (str/join " "
                     (for [[[ltable lcolumn] [rtable rcolumn]] joins]
                       (str "LEFT JOIN " ltable " ON "
                         (field-name ltable lcolumn) " = "
                         (field-name rtable rcolumn))))
        where-stmt (if (seq where)
                     (jdbc.builder/by-keys where :where {})
                     [""])
        group-sql  (if (seq group)
                     (str "GROUP BY "
                       (str/join ", "
                         (for [[table column] group]
                           (field-name table column))))
                     "")]
    (into
      [(format "SELECT %s FROM %s %s %s %s" fields-sql from join-sql
         (first where-stmt) group-sql)]
      (rest where-stmt))))

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
  (let [nested-fields        (group-by ::parent-key (filter ::parent-key fields))
        clean-left-join-nils (fn [children]
                               (if (and (= (count children) 1)
                                     (every? nil? (vals (first children))))
                                 (empty children) children))
        first-if-one         (fn [fields children]
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
  [data-source stmt opts]
  (jdbc.sql/query data-source
    stmt
    (merge {:builder-fn sql.rs/as-qualified-maps
            :key-fn     (let [idx (sql.schema/attrs->sql-col-index
                                    (::rad.attr/key->attribute opts))]
                          (fn [table column]
                            (get idx [(clojure.string/lower-case table) (clojure.string/lower-case column)]
                              (keyword table column))))}
      opts)))


(defn column-name [attr] (or (::rad.sql/column-name attr) (name (::rad.attr/qualified-key attr))))
(defn q [v]
  (if (int? v)
    v
    (str "'" v "'")))

(defn eql-query
  "Generates and executes a query based off off an EQL query by using
  the `::rad.attr/k->attr`. There is no restriction on the number
  of joined tables, but the maximum depth of these queries are 1."
  [{::rad.attr/keys [key->attribute id-attribute] :as env} data-source eql-query where]
  (let [ast         (eql/query->ast eql-query)
        schema      (::rad.sql/schema id-attribute)
        id-key      (::rad.attr/qualified-key id-attribute)
        table-name  (::rad.sql/table id-attribute)
        where       (str " WHERE "
                      (if (map? where)
                        (str (column-name id-attribute) " = " (q (get where id-key)))
                        (str (column-name id-attribute) " IN (" (str/join "," (map (comp q id-key) where)) ")")))
        columns     (into []
                      (comp
                        (map (comp key->attribute :dispatch-key))
                        (filter #(= schema (::rad.sql/schema %)))
                        (map column-name))
                      (:children ast))
        sql         (str "SELECT " (str/join "," columns) " FROM " table-name where)
        ;; join "(SELECT ID FROM ADDRESS WHERE account_id = id) AS address_id"
        raw-results (query data-source [sql] env)]
    ;; TASK: All you need for joins is something like {:account/addresses [{:address/id a} {:address/id b} ...]}
    (log/spy :debug raw-results)
    ;; TASK: general-purpose function that can take SQL result and rename keys properly.
    ;; NOTE: When processing joins, that will be a nested rename. See fix-id-keys in datomic.clj
    raw-results)
  #_(let [plan   (query->plan eql-query where env)
          stmt   (plan->sql plan)
          opts   (assoc env
                   :builder-fn sql.rs/as-maps-with-keys
                   :keys (map ::rad.attr/qualified-key (::fields plan)))
          result (jdbc.sql/query (:datasource db) stmt opts)]
      (with-meta
        (parse-executed-plan plan result)
        {::sql  (first stmt)
         ::plan plan})))
