-- =============================================================================
-- Purpose: Initialize required databases and warehouse schemas on first startup.
-- Note   : This script runs ONLY on first-time volume initialization.
--          To re-run, delete the postgres volume: docker volume rm etl_postgres_data
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section 1: Create logical databases used by Airflow, warehouse, and Metabase.
-- -----------------------------------------------------------------------------
CREATE DATABASE airflow_db;
CREATE DATABASE warehouse_db;
CREATE DATABASE metabase_db;

-- Grant full access to the application user on each database.
GRANT ALL PRIVILEGES ON DATABASE airflow_db   TO etl_admin;
GRANT ALL PRIVILEGES ON DATABASE warehouse_db TO etl_admin;
GRANT ALL PRIVILEGES ON DATABASE metabase_db  TO etl_admin;

-- -----------------------------------------------------------------------------
-- Section 2: Switch to warehouse database and create layered ETL schemas.
-- -----------------------------------------------------------------------------
\c warehouse_db;
CREATE SCHEMA IF NOT EXISTS raw;      -- landing zone, no transformation
CREATE SCHEMA IF NOT EXISTS staging;  -- cleaned and validated data
CREATE SCHEMA IF NOT EXISTS marts;    -- aggregated, business-ready data

-- Grant schema usage to the application user.
GRANT ALL ON SCHEMA raw     TO etl_admin;
GRANT ALL ON SCHEMA staging TO etl_admin;
GRANT ALL ON SCHEMA marts   TO etl_admin;