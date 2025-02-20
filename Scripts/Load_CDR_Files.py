## Importing required libraries
from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import pandas as pd
import glob
import logging
import os

# Define Required directories
PROCESSED_DIRECTORY="/home/lokesh/Telecome_Pipeline/ProcessedDir"
LOG_DIRECTORY="/home/lokesh/Telecome_Pipeline/logs"
AUDIT_DIRECTORY="/home/lokesh/Telecome_Pipeline/Audit"

# Ensure log and audit directories exist
os.makedirs(LOG_DIRECTORY, exist_ok=True)
os.makedirs(PROCESSED_DIRECTORY, exist_ok=True)

# Define logging function
def log_message(message):
    log_file = f"{LOG_DIRECTORY}/log_{date.today()}.log"
    with open(log_file, "a") as log:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.write(f"[{timestamp}] {message}\n")
    print(message)

# Creating Spark session
log_message("=====================================================================================================================")
try:
    spark= SparkSession.builder.appName("Loading Processed data into PostgreSQL").getOrCreate()
    log_message("Task L ===============================================================================================================")
    log_message("Spark session started successfully.")
except Exception as e:
    log_message(f"ERROR: Failed to initialize Spark session - {str(e)}")
    exit(1)

# Define PostgreSQL connection properties
postgres_conn = BaseHook.get_connection("postgresql_conn")
postgres_url = f"jdbc:postgresql://{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
postgres_properties = {
    "user": postgres_conn.login,
    "password": postgres_conn.password,
    "driver": "org.postgresql.Driver"
}
# Defining the Load Processed data into PostgreSQL

try:
    # Get list of CDR files
    processed_files= glob.glob(f"{PROCESSED_DIRECTORY}/Processedbatch_{date.today()}/*.csv")
    if not processed_files:
        log_message("No processed files available for loading in postgres.Exiting...")
        exit(1)
    else:
        audit_header= False
        for file in processed_files:
            try:
                log_message(f"Loading Processed file data : {file}")

                # Read CSV file into Spark DataFram
                df= spark.read.csv(file, header=True, inferSchema= True)

                # Count initial records
                procfiles_count= df.count()

                # Log audit info
                audit_procfile_data = [(os.path.basename(file), procfiles_count, str(date.today()))]
                audit_columns = ["file_name", "records_count", "processed_date"]
                audit = pd.DataFrame(audit_procfile_data, columns= audit_columns)

                if audit_header == False:
                    audit.to_csv(f"{AUDIT_DIRECTORY}/Audit_Processedfiles_{date.today()}.csv", mode= "w", index=False, header=True)
                    audit_header= True
                else:
                    audit.to_csv(f"{AUDIT_DIRECTORY}/Audit_Processedfiles_{date.today()}.csv", mode= "a", index=False, header=False)

                # Select required columns and schema casting

                cdr_activity_df = df.selectExpr(
                                                "CAST(phone_number AS STRING)",
                                                "CAST(day_calls AS INT)",
                                                "CAST(eve_calls AS INT)",
                                                "CAST(night_calls AS INT)",
                                                "CAST(intl_calls AS INT)",
                                                "CAST(custserv_calls AS INT)",
                                                "CAST(total_charge AS FLOAT)",
                                                "CAST(processing_date AS DATE)"
                )

                cdr_details_df = df.selectExpr(
                                                "CAST(phone_number AS STRING)",
                                                "CAST(account_length AS INT)",
                                                "CAST(vmail_message AS INT)",
                                                "CAST(day_mins AS DOUBLE)",
                                                "CAST(day_charge AS DOUBLE)",
                                                "CAST(eve_mins AS DOUBLE)",
                                                "CAST(eve_charge AS DOUBLE)",
                                                "CAST(night_mins AS DOUBLE)",
                                                "CAST(night_charge AS DOUBLE)",
                                                "CAST(intl_mins AS DOUBLE)",
                                                "CAST(intl_charge AS DOUBLE)",
                                                "CAST(is_fraud AS BOOLEAN)",
                                                "CAST(processing_date AS DATE)"
                )

                cdr_fraud_analysis_df = df.selectExpr(
                                                "CAST(phone_number AS STRING)",
                                                "CAST(processing_date AS DATE)",
                                                "CAST(high_frequency AS BOOLEAN)",
                                                "CAST(short_intl_calls AS BOOLEAN)",
                                                "CAST(high_custserv_calls AS BOOLEAN)",
                                                "CAST(high_total_charge AS BOOLEAN)",
                                                "CAST(risk_level AS STRING)"
                )

                # Write data to PostgreSQL in append mode
                try:
                    cdr_activity_df.write.jdbc(url=postgres_url, table="cdr_activity", mode="append", properties=postgres_properties)
                    cdr_details_df.write.jdbc(url=postgres_url, table="cdr_details", mode="append", properties=postgres_properties)
                    cdr_fraud_analysis_df.write.jdbc(url=postgres_url, table="cdr_fraud_analysis", mode="append", properties=postgres_properties)
                    log_message(f"Date loded successfully into PostgreSQL for file: {os.path.basename(file)} | Total records: {procfiles_count}")
                except Exception as e:
                    log_message(f"ERROR: Failed to load data into PostgreSQL - {str(e)}")
                    exit(1)

            except Exception as e:
                log_message(f"ERROR: Failed to Load data from {file} - {str(e)}")
                raise
                exit(1)

    log_message("Data loaded successfully into PostgreSQL database.")

except Exception as e:
    log_message(f"ERROR: Failed to Load data into PostgreSQl - {str(e)}")
    raise
    exit(1)

finally:
    try:
        spark.stop()
        log_message("Spark session closed successfully.")
        log_message("Loading Steps are done.")
        log_message("=====================================================================================================================")
    except Exception as e:
        log_message(f"WARNING: Spark session closure failed - {str(e)}")
        raise
