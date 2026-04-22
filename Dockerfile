# -----------------------------------------------------------------------------
# Purpose: Custom Airflow image with project-specific system and Python deps.
# -----------------------------------------------------------------------------
FROM apache/airflow:2.9.3

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
# Use Airflow's official constraints file to prevent provider packages from
# pulling in incompatible Airflow versions (e.g. upgrading 2.9.3 → 3.x).
RUN PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')") \
    && pip install --no-cache-dir -r /requirements.txt \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-${PYTHON_VERSION}.txt"