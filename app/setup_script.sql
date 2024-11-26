-- This is the setup script that runs while installing a Snowflake Native App in a consumer account.
-- To write this script, you can familiarize yourself with some of the following concepts:
-- Application Roles
-- Versioned Schemas
-- UDFs/Procs
-- Extension Code
-- Refer to https://docs.snowflake.com/en/developer-guide/native-apps/creating-setup-script for a detailed understanding of this file.

CREATE APPLICATION ROLE if not exists app_admin;

CREATE OR ALTER VERSIONED SCHEMA config;
GRANT USAGE ON SCHEMA config TO APPLICATION ROLE app_admin;


CREATE OR REPLACE PROCEDURE CONFIG.register_single_callback(ref_name STRING, operation STRING, ref_or_alias STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'CLEAR' THEN
          SELECT SYSTEM$REMOVE_ALL_REFERENCES(:ref_name);
      ELSE
        RETURN 'unknown operation: ' || operation;
      END CASE;
      RETURN NULL;
    END;
  $$;

GRANT USAGE ON PROCEDURE CONFIG.register_single_callback(STRING, STRING, STRING)
TO APPLICATION ROLE app_admin;



CREATE OR REPLACE procedure CONFIG.GET_CONFIGURATION_FOR_REFERENCE(ref_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CASE (ref_name)
    WHEN 'consumer_external_access' THEN
      RETURN '{
        "type": "CONFIGURATION",
        "payload":{
          "host_ports":["http-intake.logs.us3.datadoghq.com" ,
                  "instrumentation-telemetry-intake.us3.datadoghq.com"],
          "allowed_secrets" : "LIST",
          "secret_references":["CONSUMER_SECRET"]}}';
    WHEN 'consumer_secret' THEN
      RETURN '{
        "type": "CONFIGURATION",
        "payload":{
          "type" : "GENERIC_STRING"}}';
    END CASE;
  RETURN '';
END;
$$;

GRANT USAGE ON PROCEDURE CONFIG.GET_CONFIGURATION_FOR_REFERENCE(STRING)
TO APPLICATION ROLE app_admin;

CREATE OR ALTER VERSIONED SCHEMA CORE;

CREATE OR REPLACE PROCEDURE CORE.CREATE_EAI_OBJECTS()
RETURNS STRING
LANGUAGE SQL
AS 
$$
BEGIN

create function if not exists CORE.UPLOAD_TO_DATADOG(RECORD_TYPE VARCHAR,PAYLOAD OBJECT)
returns table (status varchar)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-telemetry-python', 'opentelemetry-api','requests','protobuf')
IMPORTS = ('/app.zip')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('consumer_external_access'))
--SECRETS = ('api_key' = reference('consumer_secret'))
HANDLER = 'datadog_uploader.DatadogUploader';

GRANT USAGE ON function CORE.UPLOAD_TO_DATADOG(VARCHAR,OBJECT) TO APPLICATION ROLE app_admin;

RETURN 'SUCCESS';
END;	
$$
;

grant usage on schema CORE 
to application role app_admin;
