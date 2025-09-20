from pyspark.sql import SparkSession 

def get_spark(): 
    return (SparkSession.builder.appName("TelcoChurnPipeline").getOrCreate())

#paths
BLOB_ACCOUNT_NAME = "account" 
BLOB_CONTAINER = "container" 
BLOB_SAS_TOKEN = "sas_token" 

RAW_PATH = f"wasbs://{BLOB_CONTAINER}@{BLOB_ACCOUNT_NAME}.blob.core.windows.net/raw/telco_churn.csv" 
PROCESSED_PATH = f"wasbs://{BLOB_CONTAINER}@{BLOB_ACCOUNT_NAME}.blob.core.windows.net/processed/telco_churn.parquet" 
MODEL_PATH = f"wasbs://{BLOB_CONTAINER}@{BLOB_ACCOUNT_NAME}.blob.core.windows.net/models/churn_model"