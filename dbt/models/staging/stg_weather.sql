WITH raw_data AS (
    SELECT * FROM {{ source('snowflake_kafka', 'weather_raw') }}
)

SELECT
    -- Normalisation du temps (Point 1)
    raw_data:event_time_utc::TIMESTAMP_TZ AS timestamp_brut,
    DATE(raw_data:event_time_utc::TIMESTAMP_TZ) AS date_obs,
    EXTRACT(HOUR FROM raw_data:event_time_utc::TIMESTAMP_TZ) AS heure_obs,
    
    -- Normalisation des chaînes (Point 1)
    raw_data:city::STRING AS ville,
    LOWER(REPLACE(raw_data:city::STRING, ' ', '_')) AS city_slug,
    raw_data:country::STRING AS pays,

    -- Données numériques & Aplatissement (Point 1)
    raw_data:temp_c::FLOAT AS temp_c,
    raw_data:humidity_pct::INT AS humidite_pct,
    raw_data:wind_speed_ms::FLOAT AS wind_speed_ms,
    raw_data:coord.lat::FLOAT AS latitude,
    raw_data:coord.lon::FLOAT AS longitude,

    -- Contrôles Qualité (Point 2) : Création de flags
    CASE 
        WHEN raw_data:temp_c::FLOAT BETWEEN -50 AND 60 
        AND raw_data:humidity_pct::INT BETWEEN 0 AND 100 
        THEN TRUE ELSE FALSE 
    END AS is_valid_record

FROM raw_data