-- Pregunta: ¿Cómo es la distribución de edades por país y operador?

with edades as (
    select
        country,
        operator,
        case
            when age < 18 then '<18'
            when age between 18 and 25 then '18-25'
            when age between 26 and 35 then '26-35'
            when age between 36 and 50 then '36-50'
            when age > 50 then '50+'
            else 'desconocida'
        end as age_range
    from {{ ref('silver_customers') }}
)
select
    country,
    operator,
    age_range,
    count(*) as total_clientes
from edades
group by country, operator, age_range
order by country, operator, age_range
