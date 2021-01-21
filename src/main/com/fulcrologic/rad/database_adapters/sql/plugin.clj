(ns com.fulcrologic.rad.database-adapters.sql.plugin
  (:require
    [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
    [com.fulcrologic.rad.database-adapters.sql :as sql]
    [com.fulcrologic.rad.database-adapters.sql.result-set :as sql.rs]
    [com.wsscode.pathom.core :as p]
    [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]))

(sql.rs/coerce-result-sets!)

(defn pathom-plugin
  "A pathom plugin that adds the necessary SQL connections and databases to the pathom env for
  a given request. Requires a database-mapper, which is a
  `(fn [pathom-env] {schema-name connection-pool})` for a given request.

  If a given schema needs an alternate vendor database adapter, then the two-arg version should by used and should be
  passed a map from schema name to vendor adapter
  `{schema-name vendor-name}`. For example `{:production (vendor/->PostgresAdapter)}`.

  The resulting pathom-env available to all resolvers will then have:

  - `::sql.plugin/connection-pools`: The result of the database-mapper.

  This plugin should run before (be listed after) most other plugins in the plugin chain since
  it adds connection details to the parsing env.
  "
  ([database-mapper]
   (pathom-plugin database-mapper {}))
  ([database-mapper vendor-adapters]
   (let [default-adapter (vendor/->H2Adapter)]
     (p/env-wrap-plugin
       (fn [env]
         (let [database-connection-map (database-mapper env)]
           (assoc env
             ::sql/default-adapter default-adapter
             ::sql/adapters vendor-adapters
             ::sql/connection-pools database-connection-map)))))))
