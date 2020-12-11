(ns com.fulcrologic.rad.database-adapters.sql-options
  "Options supported by the SQL adapter")

(def table
  "Attribute option. The name of the database table. Use on `ao/identity? true` attributes.
   Defaults to the snake_case namespace name of the attribute."
  :com.fulcrologic.rad.database-adapters.sql/table)

(def column-name
  "Attribute option. The string name to use for the SQL column name."
  :com.fulcrologic.rad.database-adapters.sql/column-name)

(def max-length
  "Attribute option. The max length for string attributes when this adapter
   generates schema for it. Only used by the auto-generation. Defaults to 200."
  :com.fulcrologic.rad.database-adapters.sql/max-length)

(def ^:deprecated sql->form-value "DEPRECATED. See sql->model-value."
  :com.fulcrologic.rad.database-adapters.sql/sql->form-value)

(def ^:deprecated form->sql-value "DEPRECATED. See model->sql-value."
  :com.fulcrologic.rad.database-adapters.sql/form->sql-value)

(def  model->sql-value
  "Attribute option. A `(fn [clj-value] sql-value)`. When defined, the
  writes via the plugin's form save this function will call this to convert the model value into
  something acceptable to the low-level JDBC call (see `next.jdbc.sql/execute!`).

  WARNING: The keyword name of this option does not match this option name.
  is :com.fulcrologic.rad.database-adapters.sql/form->sql-value."
  :com.fulcrologic.rad.database-adapters.sql/form->sql-value)

(def sql->model-value
  "Attribute option. A `(fn [sql-value] clojure-type)`. If defined, then this
   is called when reading raw results from the database, and can be used to convert
   the database value into the correct clojure data.

   WARNING: The keyword name of this option does not match this option name.
   is :com.fulcrologic.rad.database-adapters.sql/sql->form-value."
  :com.fulcrologic.rad.database-adapters.sql/sql->form-value)

(def connection-pools
  "Env key. This is the key under which your database connection(s) will appear
   in your Pathom resolvers when you use the pathom-plugin (see that docstring).

   This is actually a value that you generate, since when you install the pathom
   plugin you must provide it with a database mapping function. This key is
   where that database map is placed in the env."
  :com.fulcrologic.rad.database-adapters.sql/connection-pools)

(def databases
  "Config file key. Defines the databases used by the application in the config files and
   the resulting config object.

   The value of this option is a map from a developer-selected name (e.g. a shard or instance
   name) to a specification for a database/schema. The *keys* of the database are *not*
   schema names (from attributes), they are meant to allow you to create more than one
   instance of a database (potentially with the same schema) where some of your users
   might be on one, and others on another.

   The values in each database config can include any keys you want, but there are some
   predefined ones used by the built-in adapter:

   * `:flyway/migrate?` - Indicate that you'd like to use Flyway to run migrations.
   * `:flyway/migrations` - A vector of Flyway migration locations. See their docs.
   * `:hikaricp/config` - A map that will be converted to properties to pass to a Hikari connection pool.
   * `:sql/auto-create-missing?` - When true, the adapter will try to generate schema for
     defined but missing attributes. NOT recommended for production use.
   *  `:sql/schema` - The RAD schema (you define) that this database should use. Any attribute
   with a declared `ao/schema` that matches this should appear in the schema of this database.

   For example:

   ```
   :com.fulcrologic.rad.database-adapters.sql/databases
     {:main {:flyway/migrate?          true
       :flyway/migrations        [\"classpath:config/sql_migrations\"]
       :hikaricp/config          {\"dataSourceClassName\"     \"org.postgresql.ds.PGSimpleDataSource\"
                                  \"dataSource.serverName\"   \"localhost\"
                                  \"dataSource.user\"         \"grp\"
                                  \"dataSource.databaseName\" \"grp\"}
       :sql/auto-create-missing? false
       :sql/schema               :production}}
   ```
   "
  :com.fulcrologic.rad.database-adapters.sql/databases)

(def delete-referent?
  "Attribute option. Only has meaning for :ref types (both cardinalities). When an
  existing reference is changed to point to a new thing, if this option is true then the thing that is no longer
   referred to will be deleted. This allows for the basic simulation of Datomic's
   isComponent flag.  This is different than SQL CASCADE, because in this case nothing
   has been deleted. The reference on which this option lives is just being changed
   to point to a different thing."
  :com.fulcrologic.rad.database-adapters.sql/delete-referent?)