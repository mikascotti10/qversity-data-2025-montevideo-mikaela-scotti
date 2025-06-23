-- Pregunta: ¿Cómo se relaciona el credit score con el comportamiento de pagos?

select
    case
        when credit_score < 400 then 'Muy bajo (<400)'
        when credit_score >= 400 and credit_score < 500 then 'Bajo (400-499)'
        when credit_score >= 500 and credit_score < 700 then 'Medio (500-699)'
        when credit_score >= 700 then 'Alto (700+)'
        else 'Desconocido'
    end as credit_score_range,
    p.payment_status,
    count(*) as cantidad_pagos
from {{ ref('silver_customers') }} s
join {{ ref('silver_payment_history') }} p
    on s.customer_id = p.customer_id
group by credit_score_range, p.payment_status
order by credit_score_range, p.payment_status


-- con esta query se puede visualizar la relación entre el rango de la credit score con el comportamiento de pagos. 