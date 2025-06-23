-- Pregunta: ¿Qué clientes tienen pagos pendientes?
select
    ph.customer_id,
    c.first_name,
    c.last_name,
    ph.payment_amount,
    ph.payment_date
from {{ ref('silver_payment_history') }} ph
join {{ ref('silver_customers') }} c
    on ph.customer_id = c.customer_id
where ph.payment_status = 'pending'
