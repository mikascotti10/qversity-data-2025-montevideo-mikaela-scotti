-- Pregunta: ¿Cuál es el ARPU (Average Revenue Per User) por tipo de plan?
select
    plan_type,
    avg(monthly_bill_usd) as arpu
from {{ ref('silver_customers') }}
group by plan_type
