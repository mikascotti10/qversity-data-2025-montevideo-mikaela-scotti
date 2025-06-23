-- Pregunta: ¿Cuál es la distribución de clientes por ubicación?

select
    country,
    count(distinct customer_id) as total_clientes
from {{ ref('silver_customers') }}
group by country
order by total_clientes desc

-- actualmente filtrando por país; también se puede por ciudad.