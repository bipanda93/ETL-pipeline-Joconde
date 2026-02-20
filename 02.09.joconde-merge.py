import polars as pl
import outils
from sqlalchemy import MetaData, Table, Column, String, Text, create_engine, insert, delete
from dotenv import load_dotenv
import logging, os
import yaml
from datetime import date

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

fichier = '/Users/macbook/Downloads/base-joconde-extrait.json'

logging.info("📖 Chargement du fichier JSON...")
df = pl.read_json(
    fichier, 
    infer_schema_length=None  # Scanner tout le fichier
)

logging.info(f"📊 {len(df)} lignes chargées")

# Afficher la liste des colonnes
logging.info(f"📋 Colonnes disponibles ({len(df.columns)}) : {df.columns}")

# Sélectionner les colonnes
df = df.select(["reference", "appellation", "ville", "date_de_mise_a_jour"])
logging.info("✂️  Colonnes sélectionnées : reference, appellation, ville, date_de_mise_a_jour")

print("\n📄 Aperçu des 10 premières lignes :")
print(df.head(10))

# Convertir la colonne en type date (gestion des erreurs)
logging.info("📅 Conversion de la colonne date_de_mise_a_jour en type Date...")

df = df.with_columns(
    pl.col("date_de_mise_a_jour")
    .str.strptime(pl.Date, "%Y-%m-%d", strict=False)  # strict=False pour ignorer les erreurs
)

# Compter les valeurs nulles après conversion
nb_nulls = df.filter(pl.col("date_de_mise_a_jour").is_null()).height
logging.info(f"⚠️  {nb_nulls} dates invalides ou nulles détectées")

# Trouver la date maximale
max_date = df.select(pl.col("date_de_mise_a_jour").max()).to_series()[0]
min_date = df.select(pl.col("date_de_mise_a_jour").min()).to_series()[0]

logging.info(f"📊 Statistiques des dates :")
logging.info(f"  - Date minimale : {min_date}")
logging.info(f"  - Date maximale : {max_date}")

# Filtrer les enregistrements après une date seuil
date_seuil = date(2025, 5, 1)
logging.info(f"🔍 Filtrage des enregistrements après le {date_seuil}")

df_filtre = df.filter(
    pl.col("date_de_mise_a_jour") > date_seuil
)

logging.info(f"✅ {len(df_filtre)} enregistrements après le {date_seuil}")
logging.info(f"📉 Ratio : {round(len(df_filtre) / len(df) * 100, 2)}% des données conservées")

print("\n📄 Aperçu des 10 premières lignes filtrées :")
print(df_filtre.head(10))

# Statistiques supplémentaires
print("\n📊 Répartition par année :")
df_avec_annee = df_filtre.with_columns(
    pl.col("date_de_mise_a_jour").dt.year().alias("annee")
)
print(df_avec_annee.group_by("annee").agg(
    pl.count().alias("nombre")
).sort("annee"))