table = """
CREATE TABLE IF NOT EXISTS rest_catalog.vod_silver.user_actions (
    action_id STRING,
    content_id STRING,
    user_id STRING,
    device_id STRING,
    country STRING,
    action ACTION,
    start TIMESTAMP,
    end TIMESTAMP,
    duration_seconds INTEGER,
    event_date TIMESTAMP, 
)
USING ICEBERG
TBLPROPERTIES (
    'check.valid_action' = "action IN ('WATCHED', 'PAUSED')"
)
PARTITION BY country,date


CREATE TABLE your_catalog.your_db.your_table_name (
    content_id STRING,
    device_id STRING,
    event_id STRING,
    event_type STRING,
    geo_location STRUCT<
        city: STRING,
        country: STRING,
        ip_address: STRING,
        latitude: DOUBLE,
        longitude: DOUBLE,
        postal_code: STRING,
        region: STRING
    >,
    marketing_attribution STRUCT<
        affiliate_id: STRING,
        campaign_name: STRING,
        experiment_id: STRING,
        referral_code: STRING,
        utm_campaign: STRING,
        utm_content: STRING,
        utm_medium: STRING,
        utm_source: STRING,
        utm_term: STRING,
        variant_id: STRING
    >,
    metadata STRUCT<
        ad_format: STRING,
        ad_id: STRING,
        ad_type: STRING,
        category: STRING,
        dropout: BOOLEAN,
        duration_seconds: BIGINT,
        new_quality: STRING,
        old_quality: STRING,
        position: BIGINT,
        position_seconds: BIGINT,
        progress_percentage: BIGINT,
        progress_seconds: BIGINT,
        revenue: DOUBLE,
        skip_after_seconds: BIGINT,
        skip_allowed: BOOLEAN
    >,
    session_id STRING,
    technical_context STRUCT<
        app_version: STRING,
        browser_type: STRING,
        browser_version: STRING,
        connection_type: STRING,
        isp: STRING,
        network_speed: DOUBLE,
        os_type: STRING,
        os_version: STRING,
        screen_resolution: STRING,
        user_agent: STRING,
        viewport_size: STRING
    >,
    timestamp BIGINT,
    user_id STRING,
    event_date DATE,
    validation_errors ARRAY<STRING>
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
  'format-version' = '2',
  'write.object-storage.enabled' = 'true'
);
"""