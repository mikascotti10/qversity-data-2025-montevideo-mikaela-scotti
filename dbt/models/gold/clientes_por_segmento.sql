-- Pregunta: ¿Qué segmentos de clientes generan el mayor revenue?
-- Ahora la query está verificando para credit_score; hay que probar con todas las columnas que se quiera analizar

select
    credit_score,
    sum(monthly_bill_usd) as total_revenue
from {{ ref('silver_customers') }}
where credit_score is not null
    and monthly_bill_usd is not null
group by credit_score
order by total_revenue desc