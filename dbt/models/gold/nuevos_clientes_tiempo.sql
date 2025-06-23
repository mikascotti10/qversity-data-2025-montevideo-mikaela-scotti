-- Pregunta: ¿Cómo cambia la distribución de nuevos clientes en el tiempo?

select
    date_trunc('month', registration_date::date) as mes,
    count(distinct customer_id) as nuevos_clientes
from {{ ref('silver_customers') }}
group by mes
order by mes
