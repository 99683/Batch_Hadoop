from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json
from pyspark.ml.feature import StringIndexerModel, OneHotEncoderModel, VectorAssembler, StandardScalerModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Créer une session Spark
spark = SparkSession.builder \
    .appName("KafkaPreprocessStore") \
    .getOrCreate()

# Charger les modèles de prétraitement depuis HDFS
indexer_smoking_model = StringIndexerModel.load("hdfs:///data/pandemic/preprocessing_models/indexer_smoking")
encoder_smoking_model = OneHotEncoderModel.load("hdfs:///data/pandemic/preprocessing_models/encoder_smoking")
indexer_gender_model = StringIndexerModel.load("hdfs:///data/pandemic/preprocessing_models/indexer_gender")
encoder_gender_model = OneHotEncoderModel.load("hdfs:///data/pandemic/preprocessing_models/encoder_gender")
scaler_model = StandardScalerModel.load("hdfs:///data/pandemic/preprocessing_models/scaler")

# Lire le flux Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "hadoop-master:9092") \
    .option("subscribe", "pandemic-stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Schéma des données JSON
schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("hypertension", IntegerType(), True),
    StructField("heart_disease", IntegerType(), True),
    StructField("avg_glucose_level", DoubleType(), True),
    StructField("bmi", StringType(), True),
    StructField("smoking_status", StringType(), True),
    StructField("gender", StringType(), True)
])

# Parser les données JSON
raw_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Prétraitement
# 1. Gérer BMI ("N/A" -> None, cast en double)
preprocessed_df = raw_df.withColumn("bmi", when(col("bmi") == "N/A", None).otherwise(col("bmi").cast("double")))

#last update 1 to handle null values !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
preprocessed_df = preprocessed_df.withColumn("smoking_status", when(col("smoking_status").isNull(), "unknown").otherwise(col("smoking_status")))
# Drop rows with NULLs in numeric columns used by VectorAssembler
num_cols = ["age", "avg_glucose_level", "bmi"]
preprocessed_df = preprocessed_df.dropna(how="any", subset=num_cols)

# 2. Indexer et encoder les colonnes catégoriques
indexed_df = indexer_smoking_model.transform(preprocessed_df)
encoded_smoking_df = encoder_smoking_model.transform(indexed_df)
indexed_gender_df = indexer_gender_model.transform(encoded_smoking_df)
encoded_df = encoder_gender_model.transform(indexed_gender_df)

# 3. Assembler et normaliser les colonnes numériques
num_cols = ["age", "avg_glucose_level", "bmi"]
assembler_num = VectorAssembler(inputCols=num_cols, outputCol="num_features", handleInvalid="skip")
num_assembled_df = assembler_num.transform(encoded_df)
scaled_df = scaler_model.transform(num_assembled_df)
print("encodage termine ! ")
print("etape 10 : Normalisation des colonnes numériques termi")

def write_to_hdfs(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
                .format("parquet") \
                .mode("append") \
                .save("hdfs:///data/pandemic/processed_streamall")
        print(f"Batch {batch_id } ecrit dans hdfs avec {batch_df.count()} lignes")

# Lancer le streaming
query = scaled_df.writeStream \
    .foreachBatch(write_to_hdfs) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_preprocess") \
    .trigger(processingTime="5 seconds") \
    .start()

print(" Streaming lancé, en attente des données...")
query.awaitTermination()

