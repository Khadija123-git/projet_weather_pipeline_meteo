{{ config(materialized='table') }}

SELECT
    location_key,
    ville,
    date_obs,
    ROUND(AVG(temp_c), 2) AS avg_temp_c,
    MIN(temp_c) AS min_temp_c,
    MAX(temp_c) AS max_temp_c,
    MAX(temp_c) - MIN(temp_c) AS thermal_amplitude,
    ROUND(AVG(humidite_pct), 2) AS avg_humidity,
    COUNT(*) AS nb_observations
FROM {{ ref('int_weather_features') }}
GROUP BY 1, 2, 3