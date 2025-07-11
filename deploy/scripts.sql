-- Prepare function which uploads events to Datadog
-- Add your Datadog API key to the secret below
create database if not exists INFRA_MANAGEMENT;
create schema if not exists INFRA_MANAGEMENT.DATADOG;
create or replace stage INFRA_MANAGEMENT.DATADOG.HANDLER;
create or replace network rule INFRA_MANAGEMENT.DATADOG.DATADOG_INGEST
    type = host_port 
    value_list = ( 'http-intake.logs.us3.datadoghq.com' ,
                    'trace.agent.us3.datadoghq.com')
    mode = egress;

create or replace secret INFRA_MANAGEMENT.DATADOG.DATADOG_API_KEY
    type = generic_string
    secret_string = $$your_api_key$$;

create or replace external access integration DATADOG_LOGGING
    allowed_network_rules = ( INFRA_MANAGEMENT.DATADOG.DATADOG_INGEST )
    allowed_authentication_secrets = ( INFRA_MANAGEMENT.DATADOG.DATADOG_API_KEY )
    enabled = true;

put file://./datadog_handlers.zip @INFRA_MANAGEMENT.DATADOG.HANDLER auto_compress = false overwrite = true;

create or replace function INFRA_MANAGEMENT.DATADOG.UPLOAD_TO_DATADOG(RECORD_TYPE VARCHAR,PAYLOAD OBJECT)
    returns table (status varchar)
    language python
    runtime_version=3.10
    external_access_integrations = (DATADOG_LOGGING)
    secrets = ('api_key' = INFRA_MANAGEMENT.DATADOG.DATADOG_API_KEY )
    imports = ('@INFRA_MANAGEMENT.DATADOG.HANDLER/datadog_handlers.zip')
    packages = ('snowflake-telemetry-python', 'opentelemetry-api','requests','protobuf')
    handler='datadog_uploader.DatadogUploader';

-- Create an event table (if you're using the default event table, you can adjust accordingly (note that change tracking is required)
create database if not exists EVENT_DB;

create schema if not exists EVENT_DB.EVENT_SCHEMA DATA_RETENTION_TIME_IN_DAYS=1;

create event table if not exists EVENT_DB.EVENT_SCHEMA.EVENT_TABLE CHANGE_TRACKING=true;

ALTER ACCOUNT SET EVENT_TABLE = EVENT_DB.EVENT_SCHEMA.EVENT_TABLE;

create or replace stream EVENT_DB.EVENT_SCHEMA.EVENTS_TABLE_STREAM 
    ON TABLE EVENT_DB.EVENT_SCHEMA.EVENT_TABLE
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

create or replace table EVENT_DB.EVENT_SCHEMA.DUMMY_MERGE_TARGET(
    COL1 varchar
);

create or replace task EVENT_DB.EVENT_SCHEMA.EVENTS_TABLE_TASK
warehouse=COMPUTE_WH
schedule='10 minute'
when SYSTEM$STREAM_HAS_DATA('EVENT_DB.EVENT_SCHEMA.EVENTS_TABLE_STREAM')
as
insert into EVENT_DB.EVENT_SCHEMA.DUMMY_MERGE_TARGET
with non_metadata as (
    select 
        date_part(EPOCH_NANOSECOND ,START_TIMESTAMP) as "date",
        COALESCE(
            RESOURCE_ATTRIBUTES:"snow.application.package.name"::varchar, 
            RESOURCE_ATTRIBUTES:"snow.application.name"::varchar, 
            'unknown') as "service",
        DATEDIFF(NANOSECOND,START_TIMESTAMP,TIMESTAMP) as "duration",
        event_stream.*
            exclude ("METADATA$ACTION","METADATA$ISUPDATE","METADATA$ROW_ID")
    from EVENT_DB.EVENT_SCHEMA.EVENTS_TABLE_STREAM event_stream
),
event_records as (
    select 
        RECORD_TYPE,
        OBJECT_INSERT(
            OBJECT_CONSTRUCT(non_metadata.*),
            'snowflake_region',CURRENT_REGION()) as RECORD
    from non_metadata
)
select 
dd_upload_result.*
from event_records,
table(INFRA_MANAGEMENT.DATADOG.UPLOAD_TO_DATADOG(event_records.RECORD_TYPE,event_records.RECORD) OVER (PARTITION BY 1)) as dd_upload_result
where dd_upload_result.status = 'will never match this'
;

alter task EVENT_DB.EVENT_SCHEMA.EVENTS_TABLE_TASK resume;

-- Optionally, once per day you can log an event to ensure that some data is being sent to Datadog
CREATE OR REPLACE PROCEDURE EVENT_DB.EVENT_SCHEMA.HEARTBEAT()
RETURNS BOOLEAN
LANGUAGE SQL
AS
BEGIN
    SYSTEM$LOG_INFO('heartbeat-'||CURRENT_REGION());
END;

create or replace task EVENT_DB.EVENT_SCHEMA.HEARTBEAT_TASK
warehouse=COMPUTE_WH
SCHEDULE = 'USING CRON 0 0 * * * UTC'
as
call EVENT_DB.EVENT_SCHEMA.HEARTBEAT();

alter task EVENT_DB.EVENT_SCHEMA.HEARTBEAT_TASK resume;

