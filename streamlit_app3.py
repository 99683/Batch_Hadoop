import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import logging
import uuid

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paramètres de connexion PostgreSQL
DB_PARAMS = {
    "dbname": "stroke_predictionall",
    "user": "postgres",
    "password": "password",  # Remplacez par votre mot de passe réel de postgres
    "host": "localhost",
    "port": "5432"
}

# Fonction pour établir une connexion à PostgreSQL
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à PostgreSQL : {e}")
        st.error(f"Erreur de connexion à PostgreSQL : {e}")
        return None

# Fonction pour récupérer les prédictions avec cache
@st.cache_data(ttl=5)  # Cache valide pendant 5 secondes
def fetch_predictions():
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        query = """
            SELECT age, hypertension, heart_disease, avg_glucose_level, bmi, 
                   smoking_status, gender, prediction, timestamp 
            FROM predictionall 
            ORDER BY timestamp DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        logger.info(f"Récupéré {len(df)} lignes depuis predictionall")
        return df
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données : {e}")
        st.error(f"Erreur lors de la récupération des données : {e}")
        conn.close()
        return pd.DataFrame()

# Titre de l'application
st.set_page_config(page_title="Tableau de bord des prédictions de AVC", layout="wide")
st.title("Tableau de bord en temps réel des prédictions de AVC")

# Sidebar pour filtres et options
st.sidebar.header("Filtres")
age_range = st.sidebar.slider("Plage d'âge", 0, 100, (0, 100))
prediction_filter = st.sidebar.multiselect("Filtrer par prédiction", [0, 1], default=[0, 1])
refresh_rate = st.sidebar.slider("Taux de rafraîchissement (secondes)", 1, 60, 5)

# Onglets pour organiser les visualisations
tab1, tab2, tab3 = st.tabs(["Vue d'ensemble", "Détails démographiques", "Analyse temporelle"])

# Fonction pour mettre à jour le tableau de bord
def update_dashboard():
    df = fetch_predictions()
    
    if df.empty:
        st.warning("Aucune donnée disponible pour le moment.")
        return

    # Appliquer les filtres
    filtered_df = df[
        (df['age'].between(age_range[0], age_range[1])) &
        (df['prediction'].isin(prediction_filter))
    ]

    # Convertir bmi en numérique
    filtered_df['bmi'] = pd.to_numeric(filtered_df['bmi'], errors='coerce')  # "N/A" devient NaN

    # Générer des clés uniques pour chaque graphique
    unique_id = str(uuid.uuid4())

    # Tab 1 : Vue d'ensemble
    with tab1:
        st.subheader("Vue d'ensemble des prédictions")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total des prédictions", filtered_df.shape[0])
        col2.metric("Risque moyen", f"{(filtered_df['prediction'].mean() * 100):.2f}%")
        col3.metric("Âge moyen", f"{filtered_df['age'].mean():.1f} ans")

        # Répartition des prédictions
        fig_pred = px.histogram(filtered_df, x="prediction", color="prediction", 
                               nbins=2, title="Répartition des prédictions (0 = Pas de risque, 1 = Risque)",
                               labels={"prediction": "Classe de prédiction"})
        st.plotly_chart(fig_pred, use_container_width=True, key=f"pred_hist_{unique_id}")

        # Nuage de points : âge vs glucose
        fig_scatter = px.scatter(filtered_df, x="age", y="avg_glucose_level", color="prediction",
                                size="bmi", hover_data=["smoking_status", "gender"],
                                title="Âge vs Niveau moyen de glucose par prédiction")
        st.plotly_chart(fig_scatter, use_container_width=True, key=f"scatter_{unique_id}")

    # Tab 2 : Détails démographiques
    with tab2:
        st.subheader("Analyse démographique")

        # Répartition par genre
        fig_gender = px.histogram(filtered_df, x="gender", color="prediction", barmode="group",
                                 title="Répartition par genre")
        st.plotly_chart(fig_gender, use_container_width=True, key=f"gender_hist_{unique_id}")

        # Répartition par statut de fumeur
        fig_smoking = px.histogram(filtered_df, x="smoking_status", color="prediction", barmode="group",
                                  title="Répartition par statut de fumeur")
        st.plotly_chart(fig_smoking, use_container_width=True, key=f"smoking_hist_{unique_id}")

        # Hypertension et maladie cardiaque
        col4, col5 = st.columns(2)
        with col4:
            fig_hypertension = px.pie(filtered_df, names="hypertension", 
                                     title="Prévalence de l'hypertension")
            st.plotly_chart(fig_hypertension, key=f"hypertension_pie_{unique_id}")
        with col5:
            fig_heart = px.pie(filtered_df, names="heart_disease", 
                              title="Prévalence des maladies cardiaques")
            st.plotly_chart(fig_heart, key=f"heart_pie_{unique_id}")

    # Tab 3 : Analyse temporelle
    with tab3:
        st.subheader("Évolution dans le temps")
        
        # Graphique temporel des prédictions
        df_time = filtered_df.groupby(filtered_df['timestamp'].dt.floor('5min')).agg({
            'prediction': 'mean',
            'age': 'mean'
        }).reset_index()
        fig_time = go.Figure()
        fig_time.add_trace(go.Scatter(x=df_time['timestamp'], y=df_time['prediction'] * 100,
                                     mode='lines+markers', name='Risque moyen (%)'))
        fig_time.add_trace(go.Scatter(x=df_time['timestamp'], y=df_time['age'],
                                     mode='lines+markers', name='Âge moyen', yaxis="y2"))
        fig_time.update_layout(
            title="Évolution du risque et de l'âge moyen au fil du temps",
            yaxis=dict(title="Risque moyen (%)"),
            yaxis2=dict(title="Âge moyen", overlaying="y", side="right")
        )
        st.plotly_chart(fig_time, use_container_width=True, key=f"time_plot_{unique_id}")

        # Tableau des dernières prédictions
        st.subheader("Dernières prédictions (10 plus récentes)")
        st.dataframe(filtered_df.head(10))

# Boucle de mise à jour
if 'running' not in st.session_state:
    st.session_state.running = True

placeholder = st.empty()
while st.session_state.running:
    with placeholder.container():
        update_dashboard()
    time.sleep(refresh_rate)

# Bouton pour arrêter (optionnel)
if st.button("Arrêter la mise à jour"):
    st.session_state.running = False
    st.write("Mise à jour arrêtée.")
