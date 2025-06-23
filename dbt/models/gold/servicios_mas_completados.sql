-- Pregunta: ¿Qué servicios son los más contratados?

select
    service,
    count(distinct customer_id) as total_usuarios
from {{ ref('silver_contracted_services') }}
group by service
order by total_usuarios desc
