# pg_dynamic

pg_dynamic is an extension for PostgreSQL that implements a dynamic type system to Postgres.


# Goal

pg_dynamic will provide users with a datatype that allows for all datatypes in a Postgres database.

The goal is to allow a user to call use all functions and operators that each datatype has.

Users should be able to use all system datatypes, functions and operators with no configuration and the goal is to support extension datatypes with as little configuration as possible.