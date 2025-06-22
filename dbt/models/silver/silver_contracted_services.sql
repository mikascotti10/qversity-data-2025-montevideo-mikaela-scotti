{{ config(materialized='table') }}

WITH src AS (
    SELECT
        (raw_json->>'customer_id')::bigint AS customer_id,
        raw_json->'contracted_services' AS contracted_services
    FROM bronze_mobile_customers
),
-- Caso array
services_array AS (
    SELECT customer_id, jsonb_array_elements_text(contracted_services) AS service
    FROM src
    WHERE jsonb_typeof(contracted_services) = 'array'
),
-- Caso string (ej: "ROAMING,SMS")
services_string AS (
    SELECT
        customer_id,
        unnest(string_to_array(TRIM(BOTH '"' FROM contracted_services::text), ',')) AS service
    FROM src
    WHERE jsonb_typeof(contracted_services) = 'string'
),
union_services AS (
    SELECT customer_id, TRIM(BOTH '"' FROM service) AS service FROM services_array
    UNION ALL
    SELECT customer_id, TRIM(BOTH '"' FROM service) AS service FROM services_string
)
SELECT DISTINCT
    us.customer_id,
    UPPER(TRIM(us.service)) AS service
FROM union_services us
-- Solo clientes válidos (que están en silver_customers)
JOIN {{ ref('silver_customers') }} c
  ON us.customer_id = c.customer_id
WHERE
    us.customer_id IS NOT NULL
    AND LENGTH(TRIM(us.service)) > 0
    AND us.service IS NOT NULL
