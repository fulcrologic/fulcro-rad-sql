(ns com.example.components.auto-resolvers
  (:require
    [com.example.components.model :refer [all-attributes]]
    [mount.core :refer [defstate]]
    [com.fulcrologic.rad.resolvers :as res]
    [com.fulcrologic.rad.database-adapters.postgresql :as psql]
    [taoensso.timbre :as log]))

(defstate automatic-resolvers
  :start
  (vec
    (concat
      (res/generate-resolvers all-attributes)
      #_(psql/generate-resolvers all-attributes :production))))
