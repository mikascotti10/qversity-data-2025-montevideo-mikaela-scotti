-- Pregunta: ¿Cuáles son las marcas de dispositivos más populares?

select
    device_brand,
    count(distinct customer_id) as total_usuarios
from {{ ref('silver_customers') }}
group by device_brand
order by total_usuarios desc
