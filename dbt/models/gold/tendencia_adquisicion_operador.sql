-- Pregunta: ¿Cuáles son las tendencias de adquisición de clientes por operador?

select
    operator,
    date_trunc('month', registration_date::date) as mes,
    count(distinct customer_id) as nuevos_clientes
from {{ ref('silver_customers') }}
group by operator, mes
order by operator, mes
