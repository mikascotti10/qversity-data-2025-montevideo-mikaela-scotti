-- Pregunta: ¿Cuál es la preferencia de marca de dispositivo por tipo de plan?

select
    plan_type,
    device_brand,
    count(distinct customer_id) as total_usuarios
from {{ ref('silver_customers') }}
group by plan_type, device_brand
order by plan_type, total_usuarios desc
