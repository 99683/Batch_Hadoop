from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(20):  # Envoie 20 lignes
    data = {
        "age": 40 + (i % 40),
        "hypertension": i % 2,
        "heart_disease": (i % 3) == 0,
        "avg_glucose_level": 80 + (i % 70),
        "bmi": "25.0" if (i % 2) == 0 else "N/A",
        "smoking_status": "smokes" if (i % 3) == 0 else "never smoked",
        "gender": "Male" if (i % 2) == 0 else "Female"
    }
    producer.send('pandemic-stream', value=data)
    print(f"Envoyé : {data}")
    time.sleep(2)

producer.close()
print("Producteur terminé - 20 lignes envoyées.")
