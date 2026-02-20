import polars as pl
import outils
from sqlalchemy import MetaData, Table, Column, String, Text, DateTime, create_engine, insert, delete, text
from dotenv import load_dotenv
import logging, os
import yaml
from datetime import datetime, timezone

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

@outils.chronometre_logging
def charger_fichier(path):
    """Charge le fichier JSON avec Polars"""
    return pl.read_json(path, infer_schema_length=None)

fichier = '/Users/macbook/Downloads/base-joconde-extrait.json'
df = charger_fichier(fichier)

logging.info(f"📊 {len(df)} lignes chargées")

# Créer la base de données si elle n'existe pas
admin_engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5434')}/"
    f"airflow"
)

with admin_engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT")
    result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = 'joconde'"))
    if not result.fetchone():
        logging.info("🔨 Création de la base de données 'joconde'")
        conn.execute(text("CREATE DATABASE joconde"))
        logging.info("✅ Base de données 'joconde' créée")

admin_engine.dispose()

# Connexion PostgreSQL
engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5434')}/"
    f"{os.getenv('POSTGRES_DB', 'joconde')}",
    executemany_mode='values_plus_batch'  # Équivalent de fast_executemany
)

logging.info("🐳 Connexion à PostgreSQL Airflow")

metadata = MetaData()

joconde_table = Table(
    "joconde", 
    metadata,
    Column("reference", String),
    Column("appellation", String),
    Column("auteur", String),
    Column("date_creation", String),
    Column("denomination", String),
    Column("region", String),
    Column("departement", String),
    Column("ville", String),
    Column("description", Text),
    # Colonnes d'audit
    Column("source_system", String),
    Column("load_timestamp_utc", DateTime),
    Column("load_process", String),
)

metadata.create_all(engine)
logging.info("✅ Table 'joconde' créée avec colonnes d'audit")

# Sélectionner uniquement les colonnes métier
colonnes_metier = ["reference", "appellation", "auteur", "date_creation", 
                   "denomination", "region", "departement", "ville", "description"]

colonnes_existantes = [col for col in colonnes_metier if col in df.columns]
logging.info(f"📌 Colonnes métier sélectionnées : {colonnes_existantes}")

df_filtered = df.select(colonnes_existantes)

# Ajouter les colonnes d'audit
load_timestamp = datetime.now(timezone.utc)
logging.info(f"🕐 Timestamp UTC : {load_timestamp}")

records = [
    {
        **row,
        "source_system": config["audit"]["source_system"],
        "load_timestamp_utc": load_timestamp,
        "load_process": config["audit"]["load_process"]
    }
    for row in df_filtered.to_dicts()
]

logging.info(f"📤 Préparation de {len(records)} enregistrements avec métadonnées d'audit")

with engine.begin() as conn:
    conn.execute(delete(joconde_table))
    logging.info("🗑️  Table vidée")
    outils.chronometre_logging_lambda(
        "Import PostgreSQL", 
        lambda: conn.execute(insert(joconde_table), records)
    )

logging.info("✅ Données importées avec métadonnées d'audit")

# Vérification avec détails d'audit
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT 
            COUNT(*) as total,
            source_system,
            load_timestamp_utc,
            load_process
        FROM joconde
        GROUP BY source_system, load_timestamp_utc, load_process
    """))
    
    for row in result:
        logging.info(f"🔍 Vérification :")
        logging.info(f"   - {row.total} lignes insérées")
        logging.info(f"   - Source : {row.source_system}")
        logging.info(f"   - Timestamp : {row.load_timestamp_utc}")
        logging.info(f"   - Process : {row.load_process}")