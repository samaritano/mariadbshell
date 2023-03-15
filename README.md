# mariadbshell
MariaDB Shell perform a multithread dump and restore of a schema:

- Create a compressed dump file for each table of the schema
- User-defined number of concurrent table dump

```mariadb_shell <log_filename> <database_server_ip> <db_user> <db_passwd> <schema_name> <dump_directory> <number_of_threads> import|export```

