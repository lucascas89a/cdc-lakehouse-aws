"""Rebuild the gold layer from silver every 15 minutes with write-audit-publish.

build   -> pins the silver versions in gold/_staging/_manifest and writes all gold tables to staging
audit   -> data quality checks on staging against those same silver versions; fails the run (and
           blocks publish) on any violation
publish -> replaces the published gold tables, one atomic Delta commit each (not atomic across tables)
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

JOB = "cd /app && python jobs/gold_build.py"


@dag(
    schedule="*/15 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2),
                  "execution_timeout": timedelta(minutes=20)},
    tags=["gold", "spark"],
    doc_md=__doc__,
)
def gold_pipeline():
    build = BashOperator(task_id="build", bash_command=f"{JOB} build")
    # A data quality failure will not fix itself on retry: fail fast and keep the last good gold.
    audit = BashOperator(task_id="audit", bash_command=f"{JOB} audit", retries=0)
    publish = BashOperator(task_id="publish", bash_command=f"{JOB} publish")
    build >> audit >> publish


gold_pipeline()
