from kafka import KafkaProducer
import json
import time
import pandas as pd

# Initialiser le producteur Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Charger le dataset (ajustez le chemin selon l'emplacement dans le conteneur)
dataset_path = 'hdfs:///data/pandemic/clean_data'  # Changez si nécessaire
df = pd.read_csv(dataset_path)

# Sélectionner les lignes 240 à 260 (index 239 à 259 car Python commence à 0)
start_line = 239  # Ligne 240 (index 239)
end_line = 259   # Ligne 260 (index 259)
selected_data = df.iloc[start_line:end_line + 1]

# Envoyer chaque ligne au topic Kafka
for index, row in selected_data.iterrows():
    # Créer un dictionnaire avec les colonnes nécessaires
    data = {
        "age": int(row['age']),  # Convertir en entier
        "hypertension": int(row['hypertension']),
        "heart_disease": int(row['heart_disease']),
        "avg_glucose_level": float(row['avg_glucose_level']),
        "bmi": str(row['bmi']) if pd.notna(row['bmi']) else "N/A",
        "smoking_status": str(row['smoking_status']),
        "gender": str(row['gender'])
    }
    
    # Envoyer les données au topic
    producer.send('pandemic-stream', value=data)
    print(f"Envoyé ligne {index + 1} : {data}")
    time.sleep(2)  # Délai de 2 secondes entre chaque envoi

# Fermer le producteur
producer.close()
print(f"Producteur terminé - {len(selected_data)} lignes envoyées (lignes 240 à 260).")

