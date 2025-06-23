-- Pregunta: ¿Cómo se comparan la media y mediana de revenue mensual por usuario según plan y operador?

select
    plan_type,
    operator,
    avg(monthly_bill_usd) as avg_revenue,
    percentile_cont(0.5) within group (order by monthly_bill_usd) as mediana_revenue
from {{ ref('silver_customers') }}
group by plan_type, operator
