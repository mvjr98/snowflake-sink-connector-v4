-- =============================================================================
-- Snowflake setup for the v4 connector (Snowpipe Streaming, high-performance).
--
-- Replace DATB200D / LZ_IH / EVENTS / KCONNECT_* with your own names.
-- Run the ACCOUNTADMIN-scoped parts with a suitably privileged role.
-- =============================================================================

USE DATABASE DATB200D;
USE SCHEMA LZ_IH;

-- -----------------------------------------------------------------------------
-- 1. Ingest table
--
-- v4 streams straight into this table. The IH_* columns carry the Kafka
-- coordinates; they are what the deduplication downstream orders by.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EVENTS_INGEST (
    -- business columns, must mirror the source table
    ID              NUMBER          NOT NULL,
    TYPE            VARCHAR(50),

    -- connector metadata
    IH_TOPIC        VARCHAR(500),
    IH_PARTITION    NUMBER,
    IH_OFFSET       NUMBER,
    IH_OP           VARCHAR(1),     -- Debezium op: c, r, u, d
    IH_DATETIME     TIMESTAMP_NTZ,
    IH_BLOCKID      VARCHAR(36)     -- only used when ingestion_only = false
);

-- The final table is only needed when ingestion_only = false.
-- With ingestion_only = true it is replaced by the dynamic table in step 5.
-- CREATE TABLE IF NOT EXISTS EVENTS (
--     ID    NUMBER NOT NULL,
--     TYPE  VARCHAR(50)
-- );

-- -----------------------------------------------------------------------------
-- 2. Pipe
--
-- Nothing to do: Snowflake creates a default pipe named 'EVENTS_INGEST-STREAMING'
-- the first time the connector opens a channel on the table. Only create a pipe
-- explicitly if you want in-flight transformations or pre-clustering, and then
-- set the connector's `pipe` config to its name.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- 3. Service user, role and grants
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS KCONNECT_ROLE;

GRANT USAGE ON DATABASE DATB200D                TO ROLE KCONNECT_ROLE;
GRANT USAGE ON SCHEMA DATB200D.LZ_IH            TO ROLE KCONNECT_ROLE;
GRANT INSERT, SELECT ON TABLE DATB200D.LZ_IH.EVENTS_INGEST TO ROLE KCONNECT_ROLE;

-- Only needed when ingestion_only = false (MERGE/DELETE and the cleanup job):
-- GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE DATB200D.LZ_IH.EVENTS TO ROLE KCONNECT_ROLE;
-- GRANT DELETE ON TABLE DATB200D.LZ_IH.EVENTS_INGEST TO ROLE KCONNECT_ROLE;
-- GRANT USAGE ON WAREHOUSE KCONNECT_WH TO ROLE KCONNECT_ROLE;

-- A PAT for a TYPE = SERVICE user only works while the user is covered by a
-- network policy. Allow the egress addresses of the Kafka Connect cluster.
CREATE NETWORK RULE IF NOT EXISTS KCONNECT_EGRESS
    MODE = INGRESS
    TYPE = IPV4
    VALUE_LIST = ('203.0.113.10/32');   -- <- your cluster's egress IPs

CREATE NETWORK POLICY IF NOT EXISTS KCONNECT_POLICY
    ALLOWED_NETWORK_RULE_LIST = (KCONNECT_EGRESS);

CREATE USER IF NOT EXISTS SVC_KCONNECT
    TYPE = SERVICE
    DEFAULT_ROLE = KCONNECT_ROLE
    COMMENT = 'Snowflake sink connector v4 (Snowpipe Streaming)';

ALTER USER SVC_KCONNECT SET NETWORK_POLICY = KCONNECT_POLICY;
GRANT ROLE KCONNECT_ROLE TO USER SVC_KCONNECT;

-- -----------------------------------------------------------------------------
-- 4. Programmatic access token
--
-- The secret is shown once, in the output of this statement. Store it in the
-- Kafka Connect secret the connector reads, under the `password` config key -
-- the same place v2/v3 already take it from.
--
-- Tokens expire: 15 days by default, 365 days maximum. Plan the rotation - the
-- connector starts failing to authenticate the moment the token lapses.
-- -----------------------------------------------------------------------------
ALTER USER SVC_KCONNECT ADD PROGRAMMATIC ACCESS TOKEN KCONNECT_PAT
    ROLE_RESTRICTION = 'KCONNECT_ROLE'
    DAYS_TO_EXPIRY = 365
    COMMENT = 'snowflake-sink-connector v4';

-- To rotate:
-- ALTER USER SVC_KCONNECT ADD PROGRAMMATIC ACCESS TOKEN KCONNECT_PAT_NEW
--     ROLE_RESTRICTION = 'KCONNECT_ROLE' DAYS_TO_EXPIRY = 365;
-- ... update the secret, restart the connector, then:
-- ALTER USER SVC_KCONNECT REMOVE PROGRAMMATIC ACCESS TOKEN KCONNECT_PAT;

-- -----------------------------------------------------------------------------
-- 5. Deduplication with a Dynamic Table (for ingestion_only = true)
--
-- Keeps the newest event per primary key and drops the keys whose last event was
-- a delete. This replaces the connector's MERGE/DELETE.
--
-- The ORDER BY assumes a given primary key always lands in the same Kafka
-- partition, which holds for Debezium topics keyed by primary key.
-- -----------------------------------------------------------------------------
USE ROLE KCONNECT_ROLE;

CREATE OR REPLACE DYNAMIC TABLE EVENTS
    TARGET_LAG = '5 minutes'
    WAREHOUSE = KCONNECT_WH
AS
SELECT * EXCLUDE (IH_TOPIC, IH_PARTITION, IH_OFFSET, IH_OP, IH_DATETIME, IH_BLOCKID)
FROM (
    SELECT *
    FROM EVENTS_INGEST
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY IH_OFFSET DESC) = 1
)
WHERE IH_OP <> 'd';

-- NOTE ON RETENTION
-- With ingestion_only = true the connector never deletes from EVENTS_INGEST: the
-- newest row per key *is* the current state, so a blanket "delete rows older than
-- N hours" would drop live records. If the ingest table needs trimming, delete
-- only superseded versions, e.g.:
--
--   DELETE FROM EVENTS_INGEST
--   WHERE (ID, IH_OFFSET) NOT IN (
--       SELECT ID, MAX(IH_OFFSET) FROM EVENTS_INGEST GROUP BY ID
--   )
--   AND IH_DATETIME < DATEADD(hour, -24, SYSDATE());
--
-- Deletes propagate into the dynamic table's incremental refresh, so validate
-- this against your own lag settings before scheduling it.

-- -----------------------------------------------------------------------------
-- 6. Monitoring
-- -----------------------------------------------------------------------------

-- committed offset token and rejected rows, per channel
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.CHANNELS(
--     TABLE_NAME => 'DATB200D.LZ_IH.EVENTS_INGEST'));

-- SHOW PIPES LIKE 'EVENTS_INGEST-STREAMING';

-- credit consumption: SNOWPIPE_STREAMING is the v4 line, compare it against the
-- WAREHOUSE_METERING + CLOUD_SERVICES the v3 pipeline used to burn
-- SELECT service_type, SUM(credits_used) AS credits
-- FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
-- WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
-- GROUP BY 1 ORDER BY 2 DESC;
