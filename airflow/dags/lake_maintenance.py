"""Daily lake housekeeping: OPTIMIZE (compact the small files left by streaming) and VACUUM
(delete files no table version references and older than the 7-day retention).

The silver stream keeps running at 06:30 UTC, so OPTIMIZE can conflict with its MERGEs. That is
expected: silver retries Delta concurrency conflicts (its writes are idempotent) and this task
retries once. Silver must never lag more than the VACUUM retention (see jobs/maintenance.py).
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    schedule="30 6 * * *",          # 06:30 UTC, outside the demo's working hours in Argentina
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),   # 45 min task + one retry must fit
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(minutes=45)},
    tags=["maintenance", "spark"],
    doc_md=__doc__,
)
def lake_maintenance():
    BashOperator(task_id="optimize_and_vacuum", bash_command="cd /app && python jobs/maintenance.py")


lake_maintenance()
