--Pregunta: ¿Cuáles son las combinaciones de servicios más populares?

select
    services_combination,
    count(distinct customer_id) as total_usuarios
from (
    select
        customer_id,
        string_agg(service, ', ' order by service) as services_combination
    from {{ ref('silver_contracted_services') }}
    group by customer_id
) t
group by services_combination
order by total_usuarios desc

