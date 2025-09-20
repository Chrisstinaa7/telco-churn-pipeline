from pyspark.sql import SparkSession 

def get_spark(): 
    return (SparkSession.builder.appName("TelcoChurnPipeline").getOrCreate())

#paths
BLOB_ACCOUNT_NAME = "account" 
BLOB_CONTAINER = "container" 
BLOB_SAS_TOKEN = "sas_token" 