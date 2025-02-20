# Dag script to do ETL on Telecome data
# Importing required libraries
from datetime import datetime, timedelta, date
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


## Defining default arguments

default_arguments= {
    "owner": "lokesh",
    "start_date": datetime(2025,2,18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

## Defining the Dag

dag= DAG(
    "Telecome_Pipeline",
    default_args= default_arguments,
    description= "ETL pipeline for Telecome data",
    schedule_interval= timedelta(days=1),
    catchup= False
)

# Definig the Scripts path
scripts_path= "/home/lokesh/Telecome_Pipeline/Scripts"

## Defining the tasks

## Task 1: Extract the CRD files from the source repository and drop it in required location with permissions to access.

# Defining the task

Extract_CDR_Files= BashOperator(
    task_id= "Extract_CDR_Files",
    bash_command= f"""
                      cd {scripts_path}
                      ./Extract_CDR_Files.sh
                      """,
    dag= dag
)

## Task 2: Transform the extracted CDR files using the Spark job.

# Defining  the task

Process_CDR_Files= SparkSubmitOperator(
    task_id= "Process_CDR_Files",
    application= f"{scripts_path}/Process_CDR_Files.py",
    conn_id= "spark_default",
    conf={"spark.master": "local[*]"},
    spark_binary="/home/lokesh/airflow_project/airflow_venv/bin/spark-submit",
    verbose=True,
    dag=dag
)

## Task 3: Load the Audit file data in the PostgreSQL database

with open(f"{scripts_path}/Load_Audit_File_Data.sql", "r") as sql_file:
    sql_query = sql_file.read()
    
# Defining the task

Load_Audit_File_Data = PostgresOperator(
    task_id="Load_Audit_File_Data",
    sql= sql_query,  # Pass the actual query instead of file path
    postgres_conn_id="postgresql_conn",
    dag=dag
)

## Task 4: Moving extracted files to the archive
Move_Extracted_Files = BashOperator(
    task_id="Move_Extracted_Files",
    bash_command=f""" 
                     cd {scripts_path}
                     ./Move_Extracted_Files.sh
                """,
    dag=dag,
)

## Task 5: Creation of required data objects in PostgreSQL

with open(f"{scripts_path}/Create_Data_Objects.sql", "r") as sql_file:
    sql_query=sql_file.read()

# Defining the task
Create_Data_Objects= PostgresOperator(
    task_id= "Create_Data_Objects",
    sql=sql_query,
    postgres_conn_id= "postgresql_conn",
    dag= dag
)

## Task 6: Load Processed files data into PostgreSQL

# Defining the task
Load_CDR_Files = SparkSubmitOperator(
    task_id= "Load_CDR_Files",
    application= f"{scripts_path}/Load_CDR_Files.py",
    conn_id= "spark_default",
    conf= {"spark.master": "local[*]"},
    spark_binary= "/home/lokesh/airflow_project/airflow_venv/bin/spark-submit",
    verbose= True,
    dag= dag
)

## Task 7: Load Audit Processed files data into PostgreSQL
with open(f"{scripts_path}/Load_Audit_ProcessedFile_Data.sql", "r") as sql_file:
    sql_query = sql_file.read()

# Defining the task
Load_Audit_ProcessedFile_Data = PostgresOperator(
    task_id="Load_Audit_ProcessedFile_Data",
    sql= sql_query,  # Pass the actual query instead of file path
    postgres_conn_id="postgresql_conn",
    dag=dag
)


## Task 8: Moving processed files to the archive
Move_Processed_Files = BashOperator(
    task_id="Move_Processed_Files",
    bash_command=f""" 
                     cd {scripts_path}
                     ./Move_Processed_Files.sh
                """,
    dag=dag,
)

## Defining the dependencies.

Extract_CDR_Files >> Process_CDR_Files >> Create_Data_Objects >> Load_CDR_Files >> Load_Audit_ProcessedFile_Data
Process_CDR_Files >> Load_Audit_File_Data
Process_CDR_Files >> Move_Extracted_Files
Load_CDR_Files >> Move_Processed_Files
