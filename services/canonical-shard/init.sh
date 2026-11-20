#!/bin/sh
set -eu
# Patroni supplies the bootstrap connection URL. psql reads application
# credentials via its environment and quotes SQL identifiers and values.
psql -X -v ON_ERROR_STOP=1 "$1" <<'SQL'
\getenv db_user POSTGRES_USER
\getenv db_password POSTGRES_PASSWORD
\getenv db_name POSTGRES_DB
CREATE ROLE :"db_user" LOGIN PASSWORD :'db_password';
CREATE DATABASE :"db_name" OWNER :"db_user";
SQL
