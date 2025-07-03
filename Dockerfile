# 1. Extend the official Airflow image
FROM apache/airflow:2.8.0-python3.11

# 2. Switch to root to install Python deps
USER airflow

# 3. Copy and install requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && pip list

# 4. Copy your DAGs, scripts, and (optionally) plugins
COPY dags/       /opt/airflow/dags/
COPY scripts/    /opt/airflow/scripts/
COPY plugins/    /opt/airflow/plugins/
