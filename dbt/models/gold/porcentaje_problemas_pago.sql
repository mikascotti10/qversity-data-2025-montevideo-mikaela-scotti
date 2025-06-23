-- Pregunta: ¿Qué porcentaje de clientes tiene problemas de pago?

with pagos as (
    select
        customer_id,
        sum(case when payment_status = 'issue' then 1 else 0 end) as pagos_con_problema
    from {{ ref('silver_payment_history') }}
    group by customer_id
)
select
    100.0 * count(distinct case when pagos_con_problema > 0 then customer_id end) / count(distinct customer_id) as porcentaje_problemas_pago
from pagos
