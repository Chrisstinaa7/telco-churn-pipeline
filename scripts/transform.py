# transform.py
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.sql.functions import col
from utils import get_spark, PROCESSED_PATH
from ingest import ingest_data

def transform_data():
    spark = get_spark()
    df = ingest_data()

    # Drop customerID (not useful)
    df = df.drop("customerID")

    # Target variable
    indexer = StringIndexer(inputCol="Churn", outputCol="label")
    df = indexer.fit(df).transform(df)

    