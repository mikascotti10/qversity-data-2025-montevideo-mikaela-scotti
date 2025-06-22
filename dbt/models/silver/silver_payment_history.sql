{{ config(materialized='table') }}

WITH src AS (
    SELECT
        (raw_json->>'customer_id')::bigint AS customer_id,
        raw_json->'payment_history' AS payment_history
    FROM bronze_mobile_customers
),
payments AS (
    SELECT
        customer_id,
        jsonb_array_elements(payment_history) AS payment
    FROM src
    WHERE payment_history IS NOT NULL
        AND jsonb_typeof(payment_history) = 'array'
),
base AS (
    SELECT
        customer_id,
        payment->>'date' AS payment_date,
        CASE 
            WHEN (payment->>'amount') ~ '^[0-9]+(\.[0-9]+)?$' 
                THEN (payment->>'amount')::numeric
            ELSE NULL
        END AS payment_amount,
        payment->>'status' AS payment_status
    FROM payments
)
SELECT DISTINCT
    b.customer_id,
    b.payment_date,
    b.payment_amount,
    b.payment_status
FROM base b
JOIN {{ ref('silver_customers') }} c
  ON b.customer_id = c.customer_id
WHERE
    b.customer_id IS NOT NULL
    AND b.payment_date IS NOT NULL
    AND b.payment_amount IS NOT NULL
    AND LENGTH(TRIM(b.payment_date)) > 0
