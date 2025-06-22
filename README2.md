# Qversity Data Project – Mikaela Scotti

## Overview

Este proyecto implementa un pipeline completo de datos utilizando la arquitectura Medallion (Bronze, Silver, Gold) sobre un dataset personalizado de telecomunicaciones. El pipeline utiliza Docker, PostgreSQL y Apache Airflow para la orquestación y dbt para la transformación y modelado de datos.

**Las métricas clave y los insights de negocio se describirán en las siguientes etapas.**


## Participante

- **Nombre:** Mikaela Scotti  
- **Email:** mikaelascotti10@gmail.com


## Bronze Layer: Descripción y lógica

La Bronze Layer representa la primera etapa de la arquitectura, centrada en la ingestión y almacenamiento de los datos en su estado original. Esto asegura la conservación íntegra de la información para las etapas posteriores del pipeline.

### ¿Qué realiza esta capa?

- **Orquestación automatizada:**  
  Un DAG de Apache Airflow ejecuta secuencialmente los siguientes pasos:
    1. Creación de la tabla `bronze_mobile_customers` en PostgreSQL.
    2. Descarga automática del dataset JSON desde un bucket público de Amazon S3.
    3. Carga de cada registro del JSON en la base, manteniendo el formato original mediante un campo `raw_json`.
- **Persistencia confiable:**  
  Cada registro se almacena junto con un timestamp de ingestión, permitiendo trazabilidad y auditoría.
- **Entorno controlado:**  
  Todo el proceso corre en un entorno Docker Compose, lo que garantiza consistencia, portabilidad y facilidad de uso.

### Estado de la etapa Bronze

- El entorno Docker está correctamente configurado y operativo.
- El DAG de Airflow ejecuta todo el flujo sin errores.
- Los datos del JSON quedan almacenados en PostgreSQL en la tabla `bronze_mobile_customers`, listos para la siguiente etapa (Silver Layer).

---

## Silver Layer: Limpieza, normalización y control de calidad

## Nota! 
Hice el orchesting con Docker porque no me estaba funcionando bien dbt fuera de Docker.

La **Silver Layer** representa la segunda etapa del pipeline, donde los datos son **limpiados, normalizados y estructurados** para su uso analítico. En esta fase, se eliminan inconsistencias, duplicados y nulos en campos clave, y se asegura la calidad de los datos mediante tests automáticos en dbt.

### ¿Qué realiza esta capa?

* **Transformación reproducible:**
  Todas las transformaciones son gestionadas con dbt, versionadas y orquestadas en Docker para máxima reproducibilidad y control.

* **Limpieza y flatten:**

  * Se **“flatten”** todos los campos anidados y arrays del JSON original, convirtiéndolos en columnas simples.
  * Se normalizan estructuras como `contracted_services` y `payment_history`, generando una fila por cada servicio o pago asociado a un cliente.
  * Se estandarizan los formatos de fechas y tipos numéricos para evitar inconsistencias.

* **Eliminación de inconsistencias:**

  * Se eliminan registros con campos críticos nulos o vacíos (`customer_id`, `registration_date`, `email`, etc.).
  * Se remueven duplicados utilizando claves naturales y el timestamp de ingestión.
  * **Integridad referencial:**
    Los modelos de pagos y servicios solo incluyen clientes válidos existentes en la tabla `silver_customers`, garantizando la consistencia entre tablas y evitando datos huérfanos.

* **Control de calidad automatizado:**

  * Todos los modelos Silver cuentan con **tests automáticos definidos en `schema.yml`**, que verifican:

    * Unicidad y no nulos en campos clave.
    * Rangos válidos en montos (`payment_amount`).
    * Integridad relacional entre tablas (`relationships`/foreign keys).
    * Que no existan strings vacíos donde no corresponde.
  * **Todos los tests de calidad se ejecutan y pasan exitosamente**.

#### Modelos Silver principales

`silver_customers` - Clientes deduplicados y limpios, tabla maestra para la etapa Silver.
`silver_contracted_services` - Servicios contratados por cliente (1 fila por servicio y cliente).
`silver_payment_history` - Historial de pagos, normalizado (1 fila por pago y cliente).
`silver_qversity_engagement` - Interacciones con contenidos, enriquecidas y validadas.

### Estado de la etapa Silver

* Todos los modelos Silver se crean exitosamente en PostgreSQL.
* Todos los tests automáticos de dbt pasan sin errores.
* La capa Silver está lista para el análisis avanzado y la construcción de métricas en la etapa Gold.

#### Diagrama ERD
![ERD de la Silver Layer](ERD.png)

https://lucid.app/lucidchart/82ca83a3-d595-4a8c-a1cb-4baec92ffca6/edit?viewport_loc=405%2C43%2C4782%2C2491%2C0_0&invitationId=inv_8d9559e0-486d-4960-949c-f542ec1af690

