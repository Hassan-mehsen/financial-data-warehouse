FROM python:3.11-slim

RUN <<EOF
    apt-get update  &&
    apt-get install -y --no-install-recommends gcc libpq-dev  &&
    apt-get clean  &&
    rm -rf /var/lib/apt/lists/*
EOF

RUN pip install --no-cache-dir dbt-postgres==1.8.2 dbt-core==1.8.4

WORKDIR /usr/app
COPY . /usr/app
