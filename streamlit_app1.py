import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import time
import uuid

# Paramètres de connexion à PostgreSQL
DB_PARAMS = {
    "dbname": "stroke_predictions_db",
    "user": "postgres",
    "password": "password",  # Remplacez par votre mot de passe réel
    "host": "localhost",     # Puisque PostgreSQL est dans le même conteneur
    "port": "5432"
}

# Fonction pour récupérer les prédictions
def fetch_predictions():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        query = "SELECT * FROM predictions ORDER BY age DESC"  # Ajustez selon vos besoins
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erreur de connexion à PostgreSQL : {e}")
        return pd.DataFrame()  # Retourne un DataFrame vide en cas d'erreur

# Titre de l'application
st.title("Suivi en temps réel des prédictions de pandémie")

# Placeholder pour les mises à jour dynamiques
placeholder = st.empty()

# Boucle pour mise à jour en temps réel
while True:
    unique_key1 = f"prediction_hist_{uuid.uuid4()}"
    unique_key2 = f"age_bar_{uuid.uuid4()}"

    # Récupérer les données
    df = fetch_predictions()

    if not df.empty:
        # Afficher les données dans un tableau
        with placeholder.container():
            st.subheader("Dernières prédictions")
            st.dataframe(df.tail(10))  # Affiche les 10 dernières lignes

            # Métriques rapides
            st.subheader("Statistiques rapides")
            col1, col2 = st.columns(2)
            col1.metric("Nombre total de prédictions", df.shape[0])
            col2.metric("Pourcentage à risque", f"{(df['prediction'].mean() * 100):.2f}%")

            # Graphique : Répartition des prédictions
            st.subheader("Répartition des prédictions (0 = Pas de risque, 1 = Risque)")
            fig = px.histogram(df, x="prediction", color="prediction", nbins=2,
                               title="Répartition des prédictions")
            st.plotly_chart(fig,key=unique_key1)

            # Graphique : Âge moyen par prédiction
            st.subheader("Âge moyen par prédiction")
            age_by_pred = df.groupby("prediction")["age"].mean().reset_index()
            fig2 = px.bar(age_by_pred, x="prediction", y="age",
                          title="Âge moyen par classe de prédiction")
            st.plotly_chart(fig2,key=unique_key2)

    else:
        st.warning("Aucune donnée disponible pour le moment.")

    # Mise à jour toutes les 5 secondes
    time.sleep(5)
