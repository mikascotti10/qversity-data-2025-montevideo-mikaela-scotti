-- Pregunta: ¿Qué porcentaje de clientes están activos/suspendidos/inactivos?

select
    status,
    100.0 * count(distinct customer_id) / (select count(distinct customer_id) from {{ ref('silver_customers') }}) as porcentaje
from {{ ref('silver_customers') }}
group by status
