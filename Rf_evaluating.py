from spark_utils import create_spark_session
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import matplotlib.pyplot as plt
import pandas as pd

# Create Spark session
spark = create_spark_session("StrokeModelEvaluation")

# Load clean data from HDFS
clean_df = spark.read.parquet("hdfs:///data/pandemic/clean_data")

# Load trained model from HDFS
model = RandomForestClassificationModel.load("hdfs:///data/pandemic/Rf_model/")

# Train/test split (use test portion)
_, test_df = clean_df.randomSplit([0.8, 0.2], seed=42)

# Make predictions
predictions = model.transform(test_df)
# Evaluate metrics
evaluator = MulticlassClassificationEvaluator(labelCol="stroke", predictionCol="prediction")

# Accuracy
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})

# Precision (positive class)
precision = evaluator.evaluate(predictions, {evaluator.metricName: "precisionByLabel", evaluator.metricLabel: 1.0})

# Recall (sensitivity)
recall = evaluator.evaluate(predictions, {evaluator.metricName: "recallByLabel", evaluator.metricLabel: 1.0})

# Specificity (recall of negative class)
specificity = evaluator.evaluate(predictions, {evaluator.metricName: "recallByLabel", evaluator.metricLabel: 0.0})

# F1-score
f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

# Print metrics
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall (Sensitivity): {recall:.4f}")
print(f"Specificity: {specificity:.4f}")
print(f"F1-score: {f1:.4f}")

# Visualize performance
metrics = {"Accuracy": accuracy, "Precision": precision, "Recall": recall, 
                   "Specificity": specificity, "F1-score": f1}
metrics_df = pd.DataFrame(list(metrics.items()), columns=["Metric", "Value"])

plt.figure(figsize=(8, 5))
plt.bar(metrics_df["Metric"], metrics_df["Value"], color="skyblue")
plt.ylim(0, 1)
plt.title("Model Performance Metrics")
plt.ylabel("Score")
for i, v in enumerate(metrics_df["Value"]):
    plt.text(i, v + 0.02, f"{v:.4f}", ha="center")
    plt.savefig("performance_plot.png")
    print("Performance plot saved as performance_plot.png")
    
# Stop Spark session
spark.stop()

