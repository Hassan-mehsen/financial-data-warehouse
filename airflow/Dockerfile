FROM apache/airflow:3.1.0-python3.11

USER root

RUN <<EOF
apt-get update 
apt-get install -y --no-install-recommends gcc libpq-dev 
apt-get clean
rm -rf /var/lib/apt/lists/*
EOF

# Add docker group with same GID as host
RUN groupadd -g 999 docker || true && \
    usermod -aG docker airflow

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt


COPY ./airflow/dags    /opt/airflow/dags
COPY ./src             /opt/airflow/src/

