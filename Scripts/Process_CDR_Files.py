## Importing required libraries

from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import glob
import logging
import os

# Define Required directories
SOURCE_DIRECTORY= f"/home/lokesh/Telecome_Pipeline/SourceInputDir/{date.today()}/"
LOG_DIRECTORY="/home/lokesh/Telecome_Pipeline/logs"
AUDIT_DIRECTORY="/home/lokesh/Telecome_Pipeline/Audit"
PROCESSED_DIRECTORY="/home/lokesh/Telecome_Pipeline/ProcessedDir"
REJECTIONS_DIR="/home/lokesh/Telecome_Pipeline/RejectionsDir"

# Ensure log and audit directories exist
os.makedirs(LOG_DIRECTORY, exist_ok=True)
os.makedirs(AUDIT_DIRECTORY, exist_ok=True)
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
    spark = SparkSession.builder.appName("Processing CDR Files Telecom Pipeline").getOrCreate()
    log_message("Task T ===============================================================================================================")
    log_message("Spark session started successfully.")
except Exception as e:
    log_message(f"ERROR: Failed to initialize Spark session - {str(e)}")
    exit(1)

try:
    # Get list of CDR files
    CDR_files= glob.glob(SOURCE_DIRECTORY+ "*.csv")

    if not CDR_files:
        log_message("No CDR files available for processing.Exiting...")
        exit(1)
    else:
        audit_header= False
        for file in CDR_files:
            try:
                log_message(f" Processing CDR_file : {file}")

                # Read CSV file into Spark DataFrame
                df= spark.read.csv(file, header= True, inferSchema= True)
                
                # Count initial records and duplicates
                cdr_count= df.count() 
                cdr_duplicates= df.groupBy(df.columns).agg(count("*").alias("count")).filter("count > 1")
                cdr_duplicates_count= cdr_duplicates.count()
                cdr_final_count= cdr_count - cdr_duplicates_count

                # Log audit info
                audit_data = [(os.path.basename(file), cdr_count, cdr_duplicates_count, cdr_final_count, str(date.today()))]
                audit_columns = ["CDR_file_name", "records_count", "duplicates_count","final_count", "Processed_Date"]
                audit = pd.DataFrame(audit_data, columns= audit_columns)
            
                if audit_header == False:
                    audit.to_csv(f"{AUDIT_DIRECTORY}/Audit_{date.today()}.csv", mode= "w", index=False, header=True)
                    audit_header= True
                else:
                    audit.to_csv(f"{AUDIT_DIRECTORY}/Audit_{date.today()}.csv", mode= "a", index=False, header=False)

                log_message(f"File processed: {os.path.basename(file)} | Total: {cdr_count} | Duplicates Removed: {cdr_duplicates_count} | Final count: {cdr_final_count}")

                # Remove duplicates (Rejection case)
                df= df.distinct()

                # Column name mappings
                columns_mapping = [
                    ("Phone Number", "phone_number"),
                    ("Account Length", "account_length"),
                    ("VMail Message", "vmail_message"),
                    ("Day Mins", "day_mins"),
                    ("Day Calls", "day_calls"),
                    ("Day Charge", "day_charge"),
                    ("Eve Mins", "eve_mins"),
                    ("Eve Calls", "eve_calls"),
                    ("Eve Charge", "eve_charge"),
                    ("Night Mins", "night_mins"),
                    ("Night Calls", "night_calls"),
                    ("Night Charge", "night_charge"),
                    ("Intl Mins", "intl_mins"),
                    ("Intl Calls", "intl_calls"),
                    ("Intl Charge", "intl_charge"),
                    ("CustServ Calls", "custserv_calls"),
                    ("isFraud", "is_fraud")  
                ]

                for old,new in columns_mapping:
                    df= df.withColumnRenamed(old,new)
                

                # Adding fraud indicators
                
                df= df.withColumn("high_frequency", (col("day_calls") > 100))
                df= df.withColumn("short_intl_calls", (col("intl_calls") > 10) & (col("intl_mins") < 5))
                df= df.withColumn("high_custserv_calls", col("custserv_calls") > 5)
                df= df.withColumn("total_charge", round(col("day_charge")+col("eve_charge")+col("night_charge")+col("intl_charge"),2))
                df= df.withColumn("high_total_charge", col("total_charge") > 120)
                df= df.withColumn("processing_date", lit(date.today()))
                
                # Assigning risk levels

                df= df.withColumn("Risk_Level", 
                        expr("""
                                CASE WHEN high_custserv_calls = true AND high_frequency = false AND short_intl_calls= false AND high_total_charge = false THEN 'Low'
                                     WHEN high_frequency = true AND high_total_charge = true AND short_intl_calls= false AND high_custserv_calls = false THEN 'Medium'
                                     ELSE 'High'
                                END
                        """)
                )
                

                # Write processed data to CSV
                if df is not None:
                    output_dir = f"{PROCESSED_DIRECTORY}/Processedbatch_{date.today()}"
                    rejections_dir = f"{REJECTIONS_DIR}/Rejectedbatch_{date.today()}"
                    df.coalesce(1).write.format("csv").option("header", "true").mode("append").save(output_dir)
                    cdr_duplicates.coalesce(1).write.format("csv").option("header", "true").mode("append").save(rejections_dir)
                    log_message(f"{os.path.basename(file)} file Data processed and written to directory {output_dir} | record_count={df.count()}")
                    log_message(f"{os.path.basename(file)} file Rejections data written to directory {rejections_dir} | rejections_count={cdr_duplicates.count()}")
                else:
                    log_message("No data to write. Processed CDR DataFrame is empty.")
                    exit(1)

            except Exception as e:
                log_message(f"ERROR processing file {file}: {str(e)}")
                exit(1)
                raise
    
    
    # Rename part files after writing
    Processed_files= glob.glob(f"{PROCESSED_DIRECTORY}/Processedbatch_{date.today()}/part-*.csv")
    for i, file in enumerate(Processed_files, start=1):
        new_filename = f"{PROCESSED_DIRECTORY}/Processedbatch_{date.today()}/CDR_Processed_{i}_{date.today()}.csv"
        os.rename(file, new_filename)
        log_message(f"Renamed {file} to {new_filename}")


except Exception as e:
    log_message(f"ERROR: Unexpected failure during processing - {str(e)}")

finally:
    try:
        spark.stop()
        log_message("Spark session closed successfully.")
        log_message("Processing Steps are done.")
        log_message("=====================================================================================================================")
    except Exception as e:
        log_message(f"WARNING: Spark session closure failed - {str(e)}")
        raise


        






