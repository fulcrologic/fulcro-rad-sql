(ns com.fulcrologic.rad.database-adapters.psql.plugin
  (:require
    [com.fulcrologic.rad.database-adapters.sql :as sql]))

(defn wrap-env
  "Env middleware to adds the necessary SQL connections and databases to the pathom env for
   a given request. Requires a database-mapper, which is a
   `(fn [pathom-env] {schema-name connection-pool})` for a given request.

  You should also pass the general config if possible, which should have an ::sql/databases key. This allows the
  correct vendor adapter to be selected for each database.

  The resulting pathom-env available to all resolvers will then have:

  - `::sql.plugin/connection-pools`: The result of the database-mapper.
  "
  ([database-mapper] (wrap-env nil database-mapper))
  ([base-wrapper database-mapper]
   (fn [env]
     (cond-> (let [database-connection-map (database-mapper env)]
               (assoc env
                 ::sql/connection-pools database-connection-map))
       base-wrapper (base-wrapper)))))

(defn pathom-plugin
  "A pathom 2 plugin that adds the necessary SQL connections and databases to the pathom env for
   a given request. Requires a database-mapper, which is a
  `(fn [pathom-env] {schema-name connection-pool})` for a given request.

  See also wrap-env.

  The resulting pathom-env available to all resolvers will then have:

  - `::sql.plugin/connection-pools`: The result of the database-mapper.

  This plugin should run before (be listed after) most other plugins in the plugin chain since
  it adds connection details to the parsing env.
  "
  ([database-mapper]
   (let [augment (wrap-env database-mapper)]
     {:com.wsscode.pathom.core/wrap-parser
      (fn env-wrap-wrap-parser [parser]
        (fn env-wrap-wrap-internal [env tx]
          (parser (augment env) tx)))})))

