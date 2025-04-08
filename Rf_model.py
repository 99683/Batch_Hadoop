from spark_utils import create_spark_session
from pyspark.sql.functions import when, col
from pyspark.ml.classification import RandomForestClassifier


# Create Spark session
spark = create_spark_session("StrokeModelTraining")

# Load clean data from HDFS
clean_df = spark.read.parquet("hdfs:///data/pandemic/clean_data")

# Calculate class weights for imbalance
count_0 = clean_df.filter(col("stroke") == 0).count()
count_1 = clean_df.filter(col("stroke") == 1).count()
total = clean_df.count()
weight_0 = total / (2.0 * count_0)
weight_1 = total / (2.0 * count_1)
clean_df = clean_df.withColumn("weight", when(col("stroke") == 0, weight_0).otherwise(weight_1))

# Train/test split
train_df, _ = clean_df.randomSplit([0.8, 0.2], seed=42)

# Define and train Random Forest model
rf = RandomForestClassifier(featuresCol="features", labelCol="stroke", weightCol="weight", 
                                    numTrees=100, seed=42)
model = rf.fit(train_df)

# Save model to HDFS
model.write().overwrite().save("hdfs:///data/pandemic/Rf_model/")
print("Model trained successfully and saved to hdfs:///data/pandemic/Rf_model/")

# Stop Spark session
spark.stop()




