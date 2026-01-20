{{ config(materialized='incremental', unique_key='fact_key') }}

SELECT
    fact_key,
    location_key,
    timestamp_brut,
    date_obs,
    temp_c,
    wind_kmh,
    humidite_pct,
    conditions,
    is_rain,
    is_weekend
FROM {{ ref('int_weather_features') }}

{% if is_incremental() %}
  WHERE timestamp_brut > (SELECT MAX(timestamp_brut) FROM {{ this }})
{% endif %}