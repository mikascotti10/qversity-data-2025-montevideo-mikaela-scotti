# Qversity Data Project – Mikaela Scotti

## Overview

Este proyecto implementa un pipeline completo de datos utilizando la arquitectura Medallion (Bronze, Silver, Gold) sobre un dataset personalizado de telecomunicaciones. El pipeline utiliza Docker, PostgreSQL y Apache Airflow para la orquestación y dbt para la transformación y modelado de datos.

- **Bronze Layer**: Ingesta de datos en crudo. Los datos se extraen tal como vienen de un bucket público de S3 y se almacenan en PostgreSQL sin transformación, garantizando trazabilidad y auditoría completa.
- **Silver Layer**: [Por completar en la próxima fase.]
- **Gold Layer**: [Por completar en la próxima fase.]

**Las métricas clave y los insights de negocio se describirán en las siguientes etapas.**

---

## Participante

- **Nombre:** Mikaela Scotti  
- **Email:** mikaelascotti10@gmail.com

---

## Bronze Layer: Descripción y lógica

La **Bronze Layer** representa la primera etapa de la arquitectura, centrada en la ingestión y almacenamiento de los datos en su estado original. Esto asegura la conservación íntegra de la información para las etapas posteriores del pipeline.

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

