{{ config(materialized='table') }}

WITH src AS (
    SELECT
        raw_json,
        ingestion_timestamp
    FROM bronze_mobile_customers
),
base AS (
    SELECT
        (raw_json->>'customer_id')::bigint AS customer_id,
        raw_json->>'first_name' AS first_name,
        raw_json->>'last_name' AS last_name,
        raw_json->>'email' AS email,
        raw_json->>'city' AS city,
        raw_json->>'country' AS country,
        raw_json->>'operator' AS operator,
        raw_json->>'status' AS status,
        raw_json->>'plan_type' AS plan_type,
        raw_json->>'device_brand' AS device_brand,
        raw_json->>'device_model' AS device_model,
        raw_json->>'phone_number' AS phone_number,

        -- Valor numérico de edad como decimal
        CASE
            WHEN (raw_json->>'age') ~ '^\d+(\.\d+)?$' THEN (raw_json->>'age')::numeric
            ELSE NULL
        END AS age,

        -- credit_limit
        CASE
            WHEN (raw_json->>'credit_limit') ~ '^\d+(\.\d+)?$' THEN (raw_json->>'credit_limit')::numeric
            ELSE NULL
        END AS credit_limit,

        -- credit_score (entero, filtra solo enteros válidos)
        CASE
            WHEN (raw_json->>'credit_score') ~ '^\d+$' THEN (raw_json->>'credit_score')::integer
            ELSE NULL
        END AS credit_score,

        -- monthly_data_gb
        CASE
            WHEN (raw_json->>'monthly_data_gb') ~ '^\d+(\.\d+)?$' THEN (raw_json->>'monthly_data_gb')::numeric
            ELSE NULL
        END AS monthly_data_gb,

        -- monthly_bill_usd
        CASE
            WHEN (raw_json->>'monthly_bill_usd') ~ '^\d+(\.\d+)?$' THEN (raw_json->>'monthly_bill_usd')::numeric
            ELSE NULL
        END AS monthly_bill_usd,

        -- data_usage_current_month
        CASE
            WHEN (raw_json->>'data_usage_current_month') ~ '^\d+(\.\d+)?$' THEN (raw_json->>'data_usage_current_month')::numeric
            ELSE NULL
        END AS data_usage_current_month,

        CASE
            WHEN raw_json->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}$'
                THEN raw_json->>'registration_date'
            WHEN raw_json->>'registration_date' ~ '^\d{2}-\d{2}-\d{4}$'
                THEN to_char(to_date(raw_json->>'registration_date', 'MM-DD-YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END AS registration_date,

        raw_json->>'last_payment_date' AS last_payment_date,
        raw_json->>'record_uuid' AS record_uuid,
        ingestion_timestamp
    FROM src
),
deduped AS (
    SELECT DISTINCT ON (customer_id)
        *
    FROM base
    WHERE
        customer_id IS NOT NULL
        AND registration_date IS NOT NULL
        AND email IS NOT NULL  -- Opcional: puedes quitar esto si algunos clientes no tienen email
    ORDER BY customer_id, ingestion_timestamp DESC
)
SELECT * FROM deduped
