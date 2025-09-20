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
# Evaluator 
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC") 
    print("LogReg AUC:", evaluator.evaluate(lr_preds)) 
    print("RandomForest AUC:", evaluator.evaluate(rf_preds)) 
# Save the best model (say RF) 
    rf_model.save(MODEL_PATH) 
    print("Model saved at:", MODEL_PATH) 
    if __name__ == "__main__": 
        train_model()