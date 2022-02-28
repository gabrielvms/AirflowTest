from operator import is_
from airflow import DAG
from datetime import datetime as dtt
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import utils as ut


with DAG("airflow_test", start_date=dtt(2022,2,25),
            schedule_interval='30****', catchup=False, default_args={"owner":"airflow"}) as dag:

    catch_and_count_data = PythonOperator(
        task_id = "catch_and_count_data",
        python_callable = ut.catch_and_count_data
    )

    is_valid = BranchPythonOperator(
        task_id = "is_valid",
        python_callable = ut.is_valid
    )

    valid_road = BashOperator(
        task_id = "valid_road",
        bash_command = "echo 'Quantity is fine'"
    )

    invalid_road = BashOperator(
        task_id = "invalid_road",
        bash_command = "echo 'Quantity is not fine'"
    )

    catch_and_count_data >> is_valid >> [valid_road, invalid_road]