{{ config(materialized='table') }}

-- extrae y transforma datos limpios desde bronze, aplicando filtros básicos de calidad y lógica de negocio
with src as (
    select
        raw_json,
        ingestion_timestamp
    from bronze_mobile_customers
),
base as (
    select
        -- convierte a bigint y valida que no sea nulo más adelante
        (raw_json->>'customer_id')::bigint as customer_id,

        raw_json->>'first_name' as first_name,
        raw_json->>'last_name' as last_name,

        -- email: valida que no sea nulo y contenga arroba
        raw_json->>'email' as email,

        raw_json->>'city' as city,
        raw_json->>'country' as country,
        raw_json->>'operator' as operator,

        -- status normalizado a inglés/minúsculas
        case
            when lower(raw_json->>'status') in ('active', 'activo', 'válido') then 'active'
            when lower(raw_json->>'status') in ('inactive', 'inactivo', 'invalid') then 'inactive'
            when lower(raw_json->>'status') = 'suspended' then 'suspended'
            else 'unknown'
        end as status,

        raw_json->>'plan_type' as plan_type,
        raw_json->>'device_brand' as device_brand,
        raw_json->>'device_model' as device_model,
        raw_json->>'phone_number' as phone_number,

        -- edad: solo valores entre 0 y 120
        case
            when (raw_json->>'age') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'age')::numeric between 0 and 120
              then (raw_json->>'age')::numeric
            else null
        end as age,

        -- credit_limit: valor numérico positivo
        case
            when (raw_json->>'credit_limit') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'credit_limit')::numeric >= 0
                then (raw_json->>'credit_limit')::numeric
            else null
        end as credit_limit,

        -- credit_score: solo enteros
        case
            when (raw_json->>'credit_score') ~ '^\d+$'
                then (raw_json->>'credit_score')::integer
            else null
        end as credit_score,

        -- monthly_data_gb: numérico positivo
        case
            when (raw_json->>'monthly_data_gb') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'monthly_data_gb')::numeric >= 0
                then (raw_json->>'monthly_data_gb')::numeric
            else null
        end as monthly_data_gb,

        -- monthly_bill_usd: monto entre 0 y 10,000 usd
        case
            when (raw_json->>'monthly_bill_usd') ~ '^\d+(\.\d+)?$'
             and (raw_json->>'monthly_bill_usd')::numeric >= 0
             and (raw_json->>'monthly_bill_usd')::numeric <= 10000
                then (raw_json->>'monthly_bill_usd')::numeric
            else null
        end as monthly_bill_usd,

        -- data_usage_current_month: numérico positivo
        case
            when (raw_json->>'data_usage_current_month') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'data_usage_current_month')::numeric >= 0
                then (raw_json->>'data_usage_current_month')::numeric
            else null
        end as data_usage_current_month,

        -- fecha de registro: válida, formato yyyy-mm-dd o mm-dd-yyyy, no en el futuro
        case
            when raw_json->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}$'
              and to_date(raw_json->>'registration_date', 'yyyy-mm-dd') <= current_date
                then raw_json->>'registration_date'
            when raw_json->>'registration_date' ~ '^\d{2}-\d{2}-\d{4}$'
              and to_date(raw_json->>'registration_date', 'mm-dd-yyyy') <= current_date
                then to_char(to_date(raw_json->>'registration_date', 'mm-dd-yyyy'), 'yyyy-mm-dd')
            else null
        end as registration_date,

        -- última fecha de pago: válida y no en el futuro
        case
            when raw_json->>'last_payment_date' ~ '^\d{4}-\d{2}-\d{2}$'
              and to_date(raw_json->>'last_payment_date', 'yyyy-mm-dd') <= current_date
                then raw_json->>'last_payment_date'
            else null
        end as last_payment_date,

        raw_json->>'record_uuid' as record_uuid,
        ingestion_timestamp,

        -- rangos de edad
        case
            when (raw_json->>'age') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'age')::numeric < 18 then 'menor de 18'
            when (raw_json->>'age') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'age')::numeric between 18 and 25 then '18-25'
            when (raw_json->>'age') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'age')::numeric between 26 and 35 then '26-35'
            when (raw_json->>'age') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'age')::numeric between 36 and 50 then '36-50'
            when (raw_json->>'age') ~ '^\d+(\.\d+)?$'
              and (raw_json->>'age')::numeric > 50 then 'mayor de 50'
            else 'desconocida'
        end as age_range,

        -- identificación de clientes activos
        case 
            when lower(raw_json->>'status') in ('active', 'activo', 'válido') then true 
            else false 
        end as is_active,

        -- identificación de nuevos clientes
        case
            when 
                (
                    (raw_json->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}$' and to_date(raw_json->>'registration_date', 'yyyy-mm-dd') > (current_date - interval '6 months'))
                    or
                    (raw_json->>'registration_date' ~ '^\d{2}-\d{2}-\d{4}$' and to_date(raw_json->>'registration_date', 'mm-dd-yyyy') > (current_date - interval '6 months'))
                )
            then true
            else false
        end as is_new_customer

    from src

    -- filtro adicional: solo emails con arroba y no nulos
    where
        (raw_json->>'email') is not null
        and (raw_json->>'email') like '%@%'
),
deduped as (
    -- elimina duplicados por customer_id: solo deja el más reciente por ingestion_timestamp
    select distinct on (customer_id)
        *
    from base
    where
        customer_id is not null -- asegura pk válida
        and registration_date is not null -- asegura fecha de registro válida
    order by customer_id, ingestion_timestamp desc
)
-- selecciona el set final, listo para capa silver
select * from deduped
