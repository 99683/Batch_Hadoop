from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, current_timestamp
from pyspark.ml.feature import StringIndexerModel, OneHotEncoderModel, VectorAssembler, StandardScalerModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaPredictStore") \
    .getOrCreate()

# Load preprocessing models from HDFS
indexer_smoking_model = StringIndexerModel.load("hdfs:///data/pandemic/preprocessing_models/indexer_smoking")
encoder_smoking_model = OneHotEncoderModel.load("hdfs:///data/pandemic/preprocessing_models/encoder_smoking")
indexer_gender_model = StringIndexerModel.load("hdfs:///data/pandemic/preprocessing_models/indexer_gender")
encoder_gender_model = OneHotEncoderModel.load("hdfs:///data/pandemic/preprocessing_models/encoder_gender")
scaler_model = StandardScalerModel.load("hdfs:///data/pandemic/preprocessing_models/scaler")
print("etape 1 : Modèles de preprocessing chargés depuis HDFS")

# Load Random Forest model
rf_model = RandomForestClassificationModel.load("hdfs:///data/pandemic/Rf_model")
print("etape 2 : Modèle Random Forest chargé depuis HDFS")

# Read Kafka stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "hadoop-master:9092") \
    .option("subscribe", "pandemic-stream") \
    .option("startingOffsets", "earliest") \
    .load()
print("etape 3 : Lecture du flux Kafka initialisée")

# Define schema
schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("hypertension", IntegerType(), True),
    StructField("heart_disease", IntegerType(), True),
    StructField("avg_glucose_level", DoubleType(), True),
    StructField("bmi", StringType(), True),
    StructField("smoking_status", StringType(), True),
    StructField("gender", StringType(), True)
])

# Parse JSON data
raw_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
print("etape 4 : Données JSON parsées")

# Preprocess
preprocessed_df = raw_df.withColumn("bmi", when(col("bmi") == "N/A", None).otherwise(col("bmi").cast("double")))
preprocessed_df = preprocessed_df.withColumn("smoking_status", when(col("smoking_status").isNull(), "unknown").otherwise(col("smoking_status")))
preprocessed_df = preprocessed_df.dropna(how="any", subset=["age", "avg_glucose_level", "bmi"])
print("etape 5 : Préprocessing terminé (conversion BMI, remplacement smoking_status, suppression des NULLs)")

# Indexer et encoder les colonnes catégoriques
indexed_df = indexer_smoking_model.transform(preprocessed_df)
print("etape 6 : Indexation de smoking_status terminée")
encoded_smoking_df = encoder_smoking_model.transform(indexed_df)
print("etape 7 : Encodage de smoking_status terminé")
indexed_gender_df = indexer_gender_model.transform(encoded_smoking_df)
print("etape 8 : Indexation de gender terminée")
encoded_df = encoder_gender_model.transform(indexed_gender_df)
print("etape 9 : Encodage de gender terminé")

# Assembler et normaliser les colonnes numériques
num_cols = ["age", "avg_glucose_level", "bmi"]
assembler_num = VectorAssembler(inputCols=num_cols, outputCol="num_features", handleInvalid="skip")
num_assembled_df = assembler_num.transform(encoded_df)
print("etape 10 : Assemblage des colonnes numériques terminé")
scaled_df = scaler_model.transform(num_assembled_df)
print("etape 11 : Normalisation des colonnes numériques terminée")

# Assembler toutes les features pour la prédiction (comme dans process.py)
feature_cols = ["scaled_num_features", "hypertension", "heart_disease", "smoking_status_enc", "gender_enc"]
cleaned_df = scaled_df.dropna(how="any", subset=feature_cols)
final_df = VectorAssembler(inputCols=feature_cols, outputCol="features").transform(cleaned_df)
print("etape 12 : Features finales assemblées")

# Faire les prédictions
predicted_df = rf_model.transform(final_df)
print("etape 13 : Prédictions effectuées par le modèle Random Forest")

# Sélectionner les colonnes à stocker
output_df = predicted_df.select("age", "hypertension", "heart_disease", "avg_glucose_level", "bmi", "smoking_status", "gender", "prediction",current_timestamp().alias("timestamp"))
print("etape 14 : Colonnes sélectionnées pour stockage")

# Write predictions to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://hadoop-master:5432/stroke_predictions_db") \
            .option("dbtable", "predictions") \
            .option("user", "postgres") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"etape 15 : Batch {batch_id} écrit dans PostgreSQL (stroke_predictions_db.predictions) avec {batch_df.count()} lignes")
    else:
        print(f"etape 15 : Batch {batch_id} vide, rien n'a été écrit")

# Lancer le streaming
query = output_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_predict") \
    .trigger(processingTime="5 seconds") \
    .start()

print("etape 16 : Streaming lancé, en attente des donnée...")

query.awaitTermination()
