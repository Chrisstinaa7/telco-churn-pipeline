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

# Categorical columns 
    categorical = [c for c, t in df.dtypes if t == "string" and c != "Churn"] 
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx") for c in categorical] 
    encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") for c in categorical] 
    
# Numerical columns 
    numeric = [c for c, t in df.dtypes if t in ["int", "double"]] 
# Assemble features 
    assembler = VectorAssembler( inputCols=[f"{c}_vec" for c in categorical] + numeric, outputCol="features" ) 
    scaled = StandardScaler(inputCol="features", outputCol="scaledFeatures")

    