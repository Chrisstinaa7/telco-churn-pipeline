from utils import get_spark, RAW_PATH 

def ingest_data(): 
    spark = get_spark() 
    df = spark.read.csv(RAW_PATH, header=True, inferSchema=True) 
    print("Raw data schema:") 
    df.printSchema() 
    return df

if __name__ == "__main__": 
    df = ingest_data() 
    df.show(5)