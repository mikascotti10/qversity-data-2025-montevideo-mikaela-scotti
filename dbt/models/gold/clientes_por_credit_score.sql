-- Pregunta: ¿Cuál es la segmentación de clientes por rangos de credit score?
select
    case
        when credit_score < 400 then 'Muy bajo (<400)'
        when credit_score >= 400 and credit_score < 500 then 'Bajo (400-499)'
        when credit_score >= 500 and credit_score < 700 then 'Medio (500-699)'
        when credit_score >= 700 then 'Alto (700+)'
        else 'Desconocido'
    end as credit_score_range,
    sum(monthly_bill_usd) as total_revenue
from {{ ref('silver_customers') }}
group by credit_score_range
order by total_revenue desc


-- Para el análisis de revenue por segmento de clientes según credit_score, definimos cuatro rangos: muy bajo (<400), bajo (400–599), medio (600–749) y alto (750+). Esta segmentación prioriza la identificación de clientes de mayor riesgo, permitiendo a la empresa focalizar acciones sobre los segmentos "muy bajo" y "bajo", que concentran la mayor probabilidad de impago y requieren estrategias diferenciadas de gestión y retención. Además, estos cortes ofrecen una granularidad adecuada para la toma de decisiones (no complejiza demasiado).