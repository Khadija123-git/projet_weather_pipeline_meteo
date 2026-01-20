# Projet de fin de semestre – Weather Data Pipeline

## Objectifs

- Construire un pipeline cloud moderne pour la collecte et le traitement de données météorologiques
- Collecter les données météo via l’API OpenWeather
- Publier les données en streaming avec Kafka
- Stocker les événements bruts sur AWS S3 (partitionnement, lifecycle)
- Intégrer Snowflake pour l’analyse et la persistance analytique
- Orchestrer les transformations avec dbt
- Visualiser et monitorer les données avec Grafana et Streamlit

## Technologies utilisées

- Python 3.11 (collecte, traitement et streaming)
- API OpenWeather
- Apache Kafka
- AWS S3 (stockage cloud, lifecycle policy)
- Snowflake (Data Warehouse)
- dbt (Data Build Tool, transformation analytique)
- Grafana (dashboarding moderne)
- Streamlit (exploration interactive)

## Organisation du dépôt

- `/src/` : scripts de collecte, transformation et chargement
- `/data/` : exemples JSON/events collectés, datasets de test
- `/config/` : fichiers config pour Kafka, S3, Snowflake, dbt...
- `/dbt/` : projet dbt complet pour la transformation et la modélisation
- `/grafana/` : captures et modèles de dashboard Grafana
- `/streamlit/` : app Streamlit principale, screenshots
- `/aws_s3/` : règles de lifecycle et scripts liés à S3
- `/snowflake/` : scripts de création de stage/table pour la connexion Snowflake
- `/schema/` : schémas d’architecture du projet, ETL, pipeline
- `/rapport.pdf` : rapport détaillé (structure, explications, schémas, difficultés)
- `README.md` : explication synthétique du projet et du dépôt

## Instructions pour exécuter

1. **Installer les librairies** nécessaires (`pip install -r requirements.txt`)
2. **Configurer les accès** API, S3, Kafka, Snowflake via les fichiers dans `/config/`
3. **Lancer la collecte météo** avec `/src/collect_weather.py`
4. **Accéder et monitorer les topics Kafka** avec `/src/kafka_producer.py`
5. **Uploader les événements vers S3** avec `/src/s3_uploader.py`
6. **Orchestrer la transformation et le reporting** via dbt (`/dbt/`) et Snowflake (`/snowflake/`)
7. **Consulter les dashboards** dans Grafana (`/grafana/`) et Streamlit (`/streamlit/app.py`)
8. **Visualiser l’architecture et le flux** dans `/schema/`
9. **Rapport** : consultez `/rapport.pdf` pour toutes les étapes détaillées, schémas et difficultés
10. **Soumettre le lien** du dépôt GitHub avant la date limite

## Membres du groupe

- Khadija Nachid Idrissi
- Rajae Fdili
- Aya Hamim
