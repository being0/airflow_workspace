from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dag_print",
    start_date=datetime(year=2019, month=1, day=1),
    end_date=datetime(year=2019, month=1, day=5),
    schedule="@daily",
):
    BashOperator(task_id="print", bash_command="echo  is running in the {{dag.dag_id}} pipeline!")
