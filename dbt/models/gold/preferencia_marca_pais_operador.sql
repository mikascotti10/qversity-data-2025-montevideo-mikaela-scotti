-- Pregunta: ¿Cuál es la preferencia de marca de dispositivo por país/operador?

select
    country,
    operator,
    device_brand,
    count(distinct customer_id) as total_usuarios
from {{ ref('silver_customers') }}
group by country, operator, device_brand
order by country, operator, total_usuarios desc
