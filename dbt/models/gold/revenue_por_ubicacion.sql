-- Pregunta: ¿Cómo se distribuye el revenue por ubicación geográfica?

select
    country,
    sum(monthly_bill_usd) as total_revenue
from {{ ref('silver_customers') }}
group by country
order by total_revenue desc
