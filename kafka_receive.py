from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder \
    .appName("KafkaReceive") \
    .getOrCreate()

# Lire le flux Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "hadoop-master:9092") \
    .option("subscribe", "pandemic-stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Fonction pour afficher le message
def process_batch(batch_df, batch_id):
    if batch_df.count() > 0:  
        print(f"Batch {batch_id} - Données bien lues avec succès : {batch_df.count()} lignes")

# Lancer le streaming
query = kafka_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
