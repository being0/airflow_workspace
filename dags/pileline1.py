from datetime import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
with DAG(
    dag_id="dag_rocket",
    start_date=datetime(year=2019, month=1, day=1),
    end_date=datetime(year=2019, month=1, day=5),
    schedule="@daily",
):
    procure_rocket_material = EmptyOperator(task_id="procure_rocket_material")
    procure_fuel = EmptyOperator(task_id="procure_fuel")
    build_stage1 = EmptyOperator(task_id="build_stage1")
    build_stage2 = EmptyOperator(task_id="build_stage2")
    build_stage3 = EmptyOperator(task_id="build_stage3")
    launch = EmptyOperator(task_id="launch")
    procure_rocket_material >>  [build_stage1, build_stage2, build_stage3] >> launch
    procure_fuel >> build_stage3 >> launch
