from datetime import datetime

from airflow.models import DAG
from airflow.operators.emty import EmptyOperator

with DAG(
    dag_id="01_rocket_1",
    start_date=datetime(year=2019, month=1, day=1),
    end_date=datetime(year=2019, month=1, day=5),
    schedule="@daily",
):
    rocketMaterial = EmptyOperator(task_id="procedure_rocket_material")
    build1 = EmptyOperator(task_id="build_stage_1")
    build2 = EmptyOperator(task_id="build_stage_2")
    build3 = EmptyOperator(task_id="build_stage_3")
    launch = EmptyOperator(task_id="launch")
    procedureFuel = EmptyOperator(task_id="procedure_fuel")

    rocketMaterial >> [build1, build2, procedureFuel >> build3] >> launch

