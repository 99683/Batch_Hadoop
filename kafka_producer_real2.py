from kafka import KafkaProducer
from spark_utils import create_spark_session
import json
import time

# Initialiser Spark
spark = create_spark_session("KafkaProducerFullRawData")

# Lire le dataset brut depuis HDFS
dataset_path = "hdfs:///data/pandemic/stroke.csv"
df = spark.read.csv(dataset_path, header=True, inferSchema=True)

# Convertir en liste de dictionnaires pour Kafka
raw_data = df.collect() 

# Initialiser le producteur Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Envoyer chaque ligne au topic
for row in raw_data:
    data = {
        "age": int(row['age']) if row['age'] is not None else 0,
        "hypertension": int(row['hypertension']) if row['hypertension'] is not None else 0,
        "heart_disease": int(row['heart_disease']) if row['heart_disease'] is not None else 0,
        "avg_glucose_level": float(row['avg_glucose_level']) if row['avg_glucose_level'] is not None else 0.0,
        "bmi": str(row['bmi']) if row['bmi'] is not None else "N/A",
        "smoking_status": str(row['smoking_status']) if row['smoking_status'] is not None else "unknown",
        "gender": str(row['gender']) if row['gender'] is not None else "unknown"
    }
    producer.send('pandemic-stream', value=data)
    print(f"Envoyé : {data}")
    time.sleep(0.1)  # Délai de 0.1 seconde pour ne pas surcharger Kafka

# Fermer le producteur et Spark
producer.close()
spark.stop()
print("done")


