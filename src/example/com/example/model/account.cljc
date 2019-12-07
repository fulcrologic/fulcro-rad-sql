(ns com.example.model.account
  (:refer-clojure :exclude [name])
  (:require
    #?@(:clj
        [[com.wsscode.pathom.connect :as pc :refer [defmutation]]]
        :cljs
        [[com.fulcrologic.fulcro.mutations :as m :refer [defmutation]]])
    [com.wsscode.pathom.connect :as pc]
    [com.fulcrologic.rad.database-adapters.postgresql :as psql]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.attributes :as attr :refer [defattr]]
    [com.fulcrologic.rad.authorization :as auth]
    [taoensso.timbre :as log]))

(defn get-all-accounts
  [db query-params]
  #?(:clj
     nil

     ))

(defattr id ::id :uuid
  {::attr/identity? true
   ::psql/schema    :production
   ::psql/table     "account"
   ::auth/authority :local})

(defattr email ::email :string
  {::psql/schema    :production
   ::psql/table     "account"
   ::attr/required? true
   ::auth/authority :local})

(defattr active? ::active? :boolean
  {::auth/authority     :local
   ::psql/table         "account"
   ::psql/schema        :production
   ::form/default-value true})

(defattr password ::password :password
  {::auth/authority :local
   ::psql/schema    :production
   ::psql/table     "account"
   ::attr/required? true})

(defattr name ::name :string
  {::auth/authority :local
   ::psql/table     "account"
   ::psql/schema    :production
   ::attr/required? true})

(defattr all-accounts ::all-accounts :ref
  {::auth/authority :local
   ::pc/output      [{::all-accounts [::id]}]
   ::pc/resolve     (fn [{:keys       [query-params] :as env
                          ::psql/keys [databases]} input]
                      (get-all-accounts (:production databases) query-params))})

#?(:clj
   (defmutation login [env {:keys [username password]}]
     {::pc/params #{:username :password}}
     (log/info "Attempt to login for " username)
     {::auth/provider  :local
      ::auth/real-user "Tony"})
   :cljs
   (defmutation login [params]
     (ok-action [{:keys [app result]}]
       (log/info "Login result" result)
       (auth/logged-in! app :local))
     (remote [env]
       (-> env
         (m/returning auth/Session)))))

(def attributes [id name email password active? all-accounts])

(comment
  (psql/automatic-schema attributes))
