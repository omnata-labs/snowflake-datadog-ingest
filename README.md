# snowflake-datadog-ingest
Loading regional data into Datadog using SPCS

Using: https://quickstarts.snowflake.com/guide/intro_to_snowpark_container_services/index.html



snow stage create --environment dev --role ACCOUNTADMIN DATADOG_TESTING.PUBLIC.PROC_HANDLERS

snow stage copy --environment dev --role ACCOUNTADMIN datadog_handlers.zip @DATADOG_TESTING.PUBLIC.PROC_HANDLERS

