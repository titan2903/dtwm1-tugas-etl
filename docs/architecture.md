<!--
Purpose: Describe system components and end-to-end ETL architecture.
-->

# Architecture

## High-Level Flow

```text
Source -> [Airflow DAG] -> raw schema -> staging schema -> marts schema -> Metabase
```

## Component Roles

- **Airflow** orchestrates extraction, transformation, and load scheduling.
- **PostgreSQL** stores Airflow metadata and the analytics warehouse.
- **Metabase** reads analytics-ready datasets from the warehouse.

## Data Flow Direction

Data moves in one direction from source systems into layered warehouse schemas:

1. **raw** receives source-aligned records.
2. **staging** applies cleaning and validation rules.
3. **marts** exposes business-ready models for reporting.
4. **Metabase** connects to marts for dashboards and self-service analytics.