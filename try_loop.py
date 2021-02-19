from airflow.models import DAG, Variable
from airflow.operators.python import ( 
    PythonOperator, BranchPythonOperator
)
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDatasetOperator, BigQueryGetDataOperator
)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.hooks import bigquery
from airflow.providers.mysql.hooks.mysql import MySqlHook
from bq_to_parquet_multiple_tables import BigQueryToParquet
from datetime import datetime, timedelta, date
import pendulum
tz = pendulum.timezone("America/New_York")
from dateutil import parser


PROJECT_ID = "bq-airflow"
DATASET_ID = "airflow_test"
TEMP_DIRECTORY_LOCATION = "temp_folder/"
DESTINATION_TABLE_NAME = "firebase_tiaa_mobile"
        

def _days_to_fetch(**context):
    execution_dt = parser.parse(context["execution_date"]) if isinstance(context["execution_date"], str) else context["execution_date"]
    days_back = 5
    dates_back = datetime.now(tz=tz) - timedelta(days=days_back)
    
    days_list = []
    if execution_dt < dates_back:
        days_list.append(execution_dt.strftime('%Y%m%d'))   
    else:
        for i in range((execution_dt - dates_back).days +1): 
            day = execution_dt - timedelta(days=i)
            days_list.append(day.strftime('%Y%m%d'))

    return context['task_instance'].xcom_push(key= 'days_to_fetch', value = days_list)
    
def _bigquery_fetch_data(**context):
    table_list = ['events_'+ table for table in context['task_instance'].xcom_pull(key = 'days_to_fetch')]   
    print('see list of days', table_list)
    bq_to_par =  BigQueryToParquet(
            task_id='bigquery_to_parquet',
            project_id = PROJECT_ID,
            dataset_id= DATASET_ID,
            table_id= table_list, 
            location= 'US',
            temp_directory_location= TEMP_DIRECTORY_LOCATION
    )
    bq_to_par.execute(dict())

def _check_mysql_table_exist(**context):
    pass
    
with DAG(dag_id = 'BigQueryFirebase_To_MySQL', 
        start_date = datetime(2021,2,1),
#          end_date = datetime(2019,1,3),
        schedule_interval = "@daily",
        tags = ['airflow-presentation', 'example']) as dag:
    
    check_partition_exist = BigQueryTableExistenceSensor(task_id ="check_partition_exists",
                                                         project_id = PROJECT_ID,
                                                         dataset_id = DATASET_ID,
                                                         table_id= "events_{{ ds_nodash }}" )
    
    
    days_to_fetch = PythonOperator(task_id = "days_to_fetch", 
                                  provide_context = True,
                                  python_callable = _days_to_fetch)
    
    
    fetch_data = PythonOperator(task_id = "fetch_data", 
                                  provide_context = True,
                                  python_callable = _bigquery_fetch_data)
    
    data_clean_up = DummyOperator(task_id = 'pii_clean_up')
    
#     check_table_exist = BranchPythonOperator(task_id = "pick_fetch", 
#                                       provide_context = True,
#                                       python_callable = _check_mysql_table_exist)
    
    
check_partition_exist >> days_to_fetch >> fetch_data >> data_clean_up
