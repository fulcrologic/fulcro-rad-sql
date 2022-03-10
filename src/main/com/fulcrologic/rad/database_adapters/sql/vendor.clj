(ns com.fulcrologic.rad.database-adapters.sql.vendor
  (:require
    [clojure.spec.alpha :as s]
    [next.jdbc :as jdbc]))

(defprotocol VendorAdapter
  (relax-constraints! [this datasource] "Try to defer constraint checking until the end of txn.")
  (add-referential-column-statement [this origin-table origin-column target-type target-table target-column]
    "Alter table and add a FK column.")
  (next-value-for-sequence [this s] "Return next value for a sequence s"))

(s/def ::adapter (s/with-gen #(satisfies? VendorAdapter %) #(s/gen #{(reify VendorAdapter)})))

(deftype H2Adapter []
  VendorAdapter
  (relax-constraints! [_ ds] (jdbc/execute! ds ["SET REFERENTIAL_INTEGRITY FALSE"]))
  (add-referential-column-statement [_ origin-table origin-column target-type target-table target-column]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s);\n"
            origin-table origin-column target-type target-table target-column))
  (next-value-for-sequence [_ s]
    (format "SELECT NEXTVAL('%s') AS id" s)))

(deftype PostgreSQLAdapter []
  VendorAdapter
  (relax-constraints! [_ ds] (jdbc/execute! ds ["SET CONSTRAINTS ALL DEFERRED"]))
  (add-referential-column-statement [_ origin-table origin-column target-type target-table target-column]
    (format "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s REFERENCES %s(%s) DEFERRABLE INITIALLY DEFERRED;\n"
            origin-table origin-column target-type target-table target-column))
  (next-value-for-sequence [_ s]
    (format "SELECT NEXTVAL('%s') AS id" s)))

(deftype MSSQLAdapter []
  VendorAdapter
  (relax-constraints! [_ _] nil)        ;; It seems mssql cannot relax rules in a transaction
  (next-value-for-sequence [_ s]
    (format "SELECT NEXT VALUE FOR [%s] AS id" s)))
