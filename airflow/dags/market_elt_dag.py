from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task, task_group

from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

src_path = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(src_path))
# import EL classe's
from src.el.el_jobs.price_changes_el_job import PriceChangesELJob
from src.el.el_jobs.shares_float_el_job import SharesFloatELJob
from src.el.el_jobs.stock_quote_el_job import StockQuoteELJob


el_jobs = [StockQuoteELJob, PriceChangesELJob, SharesFloatELJob]

def make_callable(cls):
    """Instantiates the provided EL class and returns its `run` method as a callable."""
    instance = cls()
    return instance.run

default_args = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    description="Orchestrates full ELT pipeline for Financial Market projectt",
    start_date=datetime(2025, 11, 18, 11, 0),
    schedule="0 11 * * *",  #  11:00 UTC everyday
    catchup=False,
    tags=["elt", "financial_market"],
    default_args=default_args,
)
def market_elt_dag():

    @task_group(group_id="extract_and_load_market_data")
    def extract_and_load_market_data():

        tasks = []

        for el_job in el_jobs:

            task = PythonOperator(
                task_id=f"{el_job.__name__}", python_callable=make_callable(el_job)
            )
            tasks.append(task)

        # Serialize extraction tasks to respect API rate limits (avoid parallel API calls)
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

        return tasks

    wait_task = BashOperator(task_id="wait", bash_command="sleep 5")

    dbt_build = DockerOperator(
        task_id = "dbt_build",
        image = "dbt_finance",
        command="dbt build",
        docker_url = "unix:///var/run/docker.sock",
        auto_remove="force",
        mount_tmp_dir=False,
        environment={

            "DBT_PROFILES_DIR": os.getenv("DBT_PROFILES_DIR"),
            "DBT_DB_HOST":      os.getenv("DBT_DB_HOST"),
            "DBT_DB_PORT":      os.getenv("DBT_DB_PORT"),
            "DBT_DB_USER":      os.getenv("DBT_DB_USER"),
            "DBT_DB_PASSWORD":  os.getenv("DBT_DB_PASSWORD"),
            "DBT_DB_NAME":      os.getenv("DBT_DB_NAME"),
            "DBT_DB_SCHEMA":    os.getenv("DBT_DB_SCHEMA"),
        },
        extra_hosts={"host.docker.internal": "host-gateway"},
    )

    # Orchestration
    extract_and_load_market_data() >> wait_task >> dbt_build


market_elt_dag()
