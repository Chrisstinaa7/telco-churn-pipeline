# train.py
from utils import get_spark, PROCESSED_PATH, MODEL_PATH
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def train_model():
    spark = get_spark()
    df = spark.read.parquet(PROCESSED_PATH)

    train, test = df.randomSplit([0.8, 0.2], seed=42)

# Logistic Regression 
    lr = LogisticRegression(featuresCol="scaledFeatures", labelCol="label") 
    lr_model = lr.fit(train) 
    lr_preds = lr_model.transform(test)
# Random Forest 
    rf = RandomForestClassifier(featuresCol="scaledFeatures", labelCol="label", numTrees=50) 
    rf_model = rf.fit(train) 
    rf_preds = rf_model.transform(test)