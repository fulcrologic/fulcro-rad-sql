(ns com.fulcrologic.rad.database-adapters.sql.vendor
  (:require
    [clojure.spec.alpha :as s]
    [next.jdbc :as jdbc]))

(defprotocol VendorAdapter
  (relax-constraints! [this datasource] "Try to defer constraint checking until the end of txn.")
  (add-referential-column-statement [this origin-table origin-column target-type target-table target-column]
    "Alter table and add a FK column."))

(s/def ::adapter (s/with-gen #(satisfies? VendorAdapter %) #(s/gen #{(reify VendorAdapter)})))

(deftype H2Adapter []
  VendorAdapter
  (relax-constraints! [_ _])
  (add-referential-column-statement [this origin-table origin-column target-type target-table target-column]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s);\n"
      origin-table origin-column target-type target-table target-column)))

(deftype PostgreSQLAdapter []
  VendorAdapter
  (relax-constraints! [_ ds] (jdbc/execute! ds ["SET CONSTRAINTS ALL DEFERRED"]))
  (add-referential-column-statement [this origin-table origin-column target-type target-table target-column]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s) DEFERRABLE INITIALLY DEFERRED;\n"
      origin-table origin-column target-type target-table target-column)))
