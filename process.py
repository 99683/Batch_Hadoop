from spark_utils import create_spark_session
from pyspark.sql.functions import when, col, avg,count,countDistinct, min, max, stddev
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer, StandardScaler
from pyspark.ml import Pipeline
# Create Spark session
spark = create_spark_session("StrokeDataPreprocessing")

# Load data
df = spark.read.csv("hdfs:///data/pandemic/stroke.csv", header=True, inferSchema=True)
print("data read successfully ! ")
# Show first columns before cleaning
print("---------------------------------------------------------------------------")
print("First 20 rows before cleaning:")
df.select("age", "hypertension", "heart_disease", "avg_glucose_level", "bmi", "smoking_status", "gender").show(20)

# Preprocess bmi: Replace "N/A" with None and impute with mean
df = df.withColumn("bmi", when(col("bmi") == "N/A", None).otherwise(col("bmi").cast("double")))
imputer = Imputer(inputCols=["bmi"], outputCols=["bmi_imputed"]).setStrategy("mean")
df = imputer.fit(df).transform(df).drop("bmi").withColumnRenamed("bmi_imputed", "bmi")

# Encode categorical variables
indexer_smoking = StringIndexer(inputCol="smoking_status", outputCol="smoking_status_idx",handleInvalid="keep")
encoder_smoking = OneHotEncoder(inputCol="smoking_status_idx", outputCol="smoking_status_enc")
indexer_gender = StringIndexer(inputCol="gender", outputCol="gender_idx",handleInvalid="keep")
encoder_gender = OneHotEncoder(inputCol="gender_idx", outputCol="gender_enc")

# Assemble numerical features for scaling
num_cols = ["age", "avg_glucose_level", "bmi"]
assembler_num = VectorAssembler(inputCols=num_cols, outputCol="num_features")
scaler = StandardScaler(inputCol="num_features", outputCol="scaled_num_features", withMean=True, withStd=True)

# Final feature assembly
feature_cols = ["scaled_num_features", "hypertension", "heart_disease", "smoking_status_enc", "gender_enc"]
assembler_final = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Pipeline
pipeline = Pipeline(stages=[indexer_smoking, encoder_smoking, indexer_gender, encoder_gender, 
                               assembler_num, scaler, assembler_final])
clean_df = pipeline.fit(df).transform(df)

# Select relevant columns
clean_df = clean_df.select("features", "stroke", "age", "hypertension", "heart_disease", 
                                   "avg_glucose_level", "bmi", "smoking_status", "gender")

# Show first columns after cleaning
print("First 5 rows after cleaning:")
clean_df.select("age", "hypertension", "heart_disease", "avg_glucose_level", "bmi", "smoking_status", "gender").show(20)

# Data info
print("Data information summary : ")

row_count = clean_df.count()
col_count = len(clean_df.columns)
#avg_age = clean_df.agg({"age": "avg"}).collect()[0][0]

print(f"Shape of the data: ({row_count}, {col_count})")
#print(f"Average age: {avg_age:.2f}")
numeric_cols = ["age", "avg_glucose_level", "bmi"]
stats = clean_df.agg(
    avg("age").alias("avg_age"),
    min("age").alias("min_age"),
    max("age").alias("max_age"),
    stddev("age").alias("std_age"),
    avg("avg_glucose_level").alias("avg_glucose"),
    min("avg_glucose_level").alias("min_glucose"),
    max("avg_glucose_level").alias("max_glucose"),
    avg("bmi").alias("avg_bmi"),
    min("bmi").alias("min_bmi"),
    max("bmi").alias("max_bmi")
).collect()[0]

print(f"Average age: {stats['avg_age']:.2f} (Min: {stats['min_age']}, Max: {stats['max_age']}, StdDev: {stats['std_age']:.2f})")
print(f"Average glucose level: {stats['avg_glucose']:.2f} (Min: {stats['min_glucose']}, Max: {stats['max_glucose']})")
print(f"Average BMI: {stats['avg_bmi']:.2f} (Min: {stats['min_bmi']}, Max: {stats['max_bmi']})")

hypertension_count = clean_df.filter(col("hypertension") == 1).count()
heart_disease_count = clean_df.filter(col("heart_disease") == 1).count()
print(f"Number of cases with hypertension: {hypertension_count} ({(hypertension_count / row_count * 100):.2f}%)")
print(f"Number of cases with heart disease: {heart_disease_count} ({(heart_disease_count / row_count * 100):.2f}%)")

print("\nDistribution of categorical variables:")
print("Smoking Status:")
clean_df.groupBy("smoking_status").agg(count("*").alias("count")).show()
print("Gender:")
clean_df.groupBy("gender").agg(count("*").alias("count")).show()

missing_counts = {col_name: clean_df.filter(col(col_name).isNull()).count() for col_name in numeric_cols}
print("\nMissing values after preprocessing:")
for col_name, missing in missing_counts.items():
    print(f"{col_name}: {missing} missing ({(missing / row_count * 100):.2f}%)")

# Save to HDFS
clean_df.write.parquet("hdfs:///data/pandemic/clean_data2", mode="overwrite")
print("Clean data saved to hdfs:///data/pandemic/clean_data2")

# Stop Spark session
spark.stop()


