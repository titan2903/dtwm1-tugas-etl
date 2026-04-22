<!--
Purpose: Detailed local setup guide for first-time environment initialization.
-->

# Setup Guide

## 1. Prepare Environment File

```bash
cp .env.example .env
```

Fill all required values in `.env`.

## 2. Generate Fernet Key

Run the following command and paste the output into
`AIRFLOW__CORE__FERNET_KEY` in `.env`.

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## 3. Set AIRFLOW_UID on Linux

Use your current Linux UID to avoid file permission issues on mounted volumes.

```bash
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

## 4. First-Time Database Initialization Sequence

1. Start PostgreSQL and initialize databases/schemas via mounted SQL script.
2. Run `airflow-init` one-shot service for migration and admin user bootstrap.
3. Start webserver, scheduler, and Metabase.

```bash
docker compose up --build
```

## 5. Access Service UIs

- Airflow: http://localhost:8080
- Metabase setup wizard: http://localhost:3000