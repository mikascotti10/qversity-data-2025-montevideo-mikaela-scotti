-- Pregunta: ¿Qué combinaciones de servicios generan mayor revenue?

with servicios as (
    select
        customer_id,
        string_agg(service, ', ' order by service) as servicios_comb
    from {{ ref('silver_contracted_services') }}
    group by customer_id
), revenue as (
    select
        customer_id,
        max(monthly_bill_usd) as total_revenue
    from {{ ref('silver_customers') }}
    group by customer_id
)
select
    s.servicios_comb,
    sum(r.total_revenue) as revenue_total
from servicios s
join revenue r on s.customer_id = r.customer_id
group by s.servicios_comb
order by revenue_total desc

