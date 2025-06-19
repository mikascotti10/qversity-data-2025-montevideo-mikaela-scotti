# Qversity Data Project – Mikaela Scotti

## Overview

This project implements a complete data pipeline using the Medallion Architecture (Bronze, Silver, Gold) on a custom telecommunications dataset. The pipeline leverages Docker, PostgreSQL, Apache Airflow for orchestration, and dbt for data transformation and modeling.

- **Bronze Layer**: Raw data ingestion. Data is extracted as-is from a public S3 bucket and stored in PostgreSQL without transformation, ensuring full traceability and auditability.
- **Silver Layer**: [To be completed in the next phase.]
- **Gold Layer**: [To be completed in the next phase.]

**Key insights and business metrics will be described in later stages.**

---

## Participant

- **Name**: Mikaela Scotti
- **Email**: mikaelascotti10@gmail.com

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/mikascotti10/qversity-data-2025-montevideo-mikaela-scotti.git
cd qversity-data-2025-montevideo-mikaela-scotti
