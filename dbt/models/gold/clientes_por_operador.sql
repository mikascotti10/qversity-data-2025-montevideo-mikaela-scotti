-- Pregunta: ¿Cómo se distribuyen los clientes entre diferentes operadores?

select
    operator,
    count(distinct customer_id) as total_clientes
from {{ ref('silver_customers') }}
group by operator
