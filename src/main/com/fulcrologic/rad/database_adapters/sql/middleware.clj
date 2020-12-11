(ns com.fulcrologic.rad.database-adapters.sql.middleware
  (:require
    [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.database-adapters.sql.resolvers :as sql.resolvers]))

(defn wrap-sql-save
  "Form save middleware to accomplish SQL saves."
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [save-result (sql.resolvers/save-form! pathom-env params)]
       save-result)))
  ([handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [save-result    (sql.resolvers/save-form! pathom-env params)
           handler-result (handler pathom-env)]
       (deep-merge save-result handler-result)))))


(defn wrap-sql-delete
  "Form delete middleware to accomplish SQL deletes."
  ([handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [local-result   (sql.resolvers/delete-entity! pathom-env params)
           handler-result (handler pathom-env)]
       (deep-merge handler-result local-result))))
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (sql.resolvers/delete-entity! pathom-env params))))

