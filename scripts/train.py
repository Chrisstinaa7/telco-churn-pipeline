# train.py
from utils import get_spark, PROCESSED_PATH, MODEL_PATH
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def train_model():
    spark = get_spark()
    df = spark.read.parquet(PROCESSED_PATH)

    train, test = df.randomSplit([0.8, 0.2], seed=42)

   