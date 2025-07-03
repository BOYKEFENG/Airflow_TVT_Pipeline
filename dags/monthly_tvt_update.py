from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='monthly_tvt_update',
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval='@monthly',
    catchup=False,
    tags=['tvt','monthly'],
) as dag:

    t0 = BashOperator(
        task_id='debug_list_dirs',
        bash_command="""
          echo ">>> PWD: $(pwd)";
          echo ">>> LIST /opt/airflow:";
          ls -R /opt/airflow;
          echo ">>> LIST scripts:";
          ls -R /opt/airflow/scripts
        """
    )

    t1 = BashOperator(
        task_id='download_and_prepare',
        bash_command='python /opt/airflow/scripts/data_prep.py'
    )

    t2 = BashOperator(
        task_id='merge_state_miles',
        bash_command='python /opt/airflow/scripts/merge_tvt_page456.py'
    )

    t3 = BashOperator(
        task_id='merge_national_vmt',
        bash_command='python /opt/airflow/scripts/merge_tvt_data.py'
    )

    t4 = BashOperator(
        task_id='fetch_gdp',
        bash_command='python /opt/airflow/scripts/GDP_All_Year.py'
    )

    t5 = BashOperator(
        task_id='fetch_labor_participation',
        bash_command='python /opt/airflow/scripts/Labor_Participation_Rate_1948_present.py'
    )

    t6 = BashOperator(
        task_id='fetch_cpi',
        bash_command='python /opt/airflow/scripts/CPI_1913_present.py'
    )

    t7 = BashOperator(
        task_id='fetch_unemployment',
        bash_command='python /opt/airflow/scripts/Unemployment_Rate_1948_present.py'
    )

    t8 = BashOperator(
        task_id='build_master_db',
        bash_command='python /opt/airflow/scripts/tvt_db.py'
    )

    # Define dependencies
    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
    