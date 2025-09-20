from utils import get_spark, RAW_PATH 

def ingest_data(): 
    spark = get_spark() 
    df = spark.read.csv(RAW_PATH, header=True, inferSchema=True) 
    print("Raw data schema:") 
    df.printSchema() 
    return df