{{ config(materialized='table') }}

SELECT DISTINCT
    user_id,
    qversity_id,
    CASE 
        WHEN qversity_id = 'qversity_python_fundamentals' THEN 'Python Fundamentals'
        WHEN qversity_id = 'qversity_airflow_orchestration' THEN 'Airflow Orchestration'
        WHEN qversity_id = 'qversity_dbt_transformations' THEN 'dbt Transformations'
        WHEN qversity_id = 'qversity_dwh_design' THEN 'Data Warehouse Design'
        ELSE qversity_id
    END AS qversity_name,
    content_type,
    content_id,
    content_title,
    CASE 
        WHEN content_type = 'video' THEN watch_time_seconds / 60.0
        ELSE 0 
    END AS watch_time_minutes,
    CASE 
        WHEN content_type = 'video' AND watch_time_seconds >= 900 THEN true  -- 15 min minimum for videos
        WHEN content_type IN ('document', 'quiz') AND completed THEN true
        ELSE false 
    END AS engagement_threshold_met,
    completed,
    interaction_timestamp,
    extract(hour from interaction_timestamp) as interaction_hour,
    extract(dow from interaction_timestamp) as interaction_day_of_week,
    current_timestamp as processed_at
FROM {{ ref('bronze_qversity_interactions') }}
WHERE
    user_id IS NOT NULL
    AND qversity_id IS NOT NULL
    AND LENGTH(TRIM(qversity_id)) > 0
    AND LENGTH(TRIM(user_id::text)) > 0

