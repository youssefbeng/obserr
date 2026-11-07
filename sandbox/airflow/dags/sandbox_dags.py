"""DAGs d'exemple : `sandbox_ok` réussit, `sandbox_fail` a une tâche en échec."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("sandbox_ok", start_date=datetime(2024, 1, 1), schedule=None, catchup=False):
    BashOperator(task_id="say_ok", bash_command="echo ok")

with DAG("sandbox_fail", start_date=datetime(2024, 1, 1), schedule=None, catchup=False):
    BashOperator(task_id="boom", bash_command="echo 'sandbox-boom' >&2; exit 1")
