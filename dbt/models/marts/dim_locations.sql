{{ config(materialized='table') }}

SELECT DISTINCT
    location_key,
    ville,
    city_slug,
    pays,
    latitude,
    longitude
FROM {{ ref('int_weather_features') }}