import polars as pl
import outils
from sqlalchemy import MetaData, Table, Column, String, Text, create_engine, insert, delete, text
from dotenv import load_dotenv
import logging, os
import yaml

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

# Connexion PostgreSQL avec fast executemany équivalent
engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5434')}/"
    f"{os.getenv('POSTGRES_DB', 'joconde')}",
    executemany_mode='values_plus_batch'  # Équivalent fast_executemany pour PostgreSQL
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
)

metadata.create_all(engine)
logging.info("✅ Table 'joconde' créée ou déjà existante")

# Sélectionner uniquement les colonnes de la table
colonnes_table = ["reference", "appellation", "auteur", "date_creation", 
                  "denomination", "region", "departement", "ville", "description"]

colonnes_existantes = [col for col in colonnes_table if col in df.columns]
logging.info(f"📌 Colonnes sélectionnées : {colonnes_existantes}")

df = df.select(colonnes_existantes)
records = df.to_dicts()

logging.info(f"📤 Préparation de {len(records)} enregistrements")

with engine.begin() as conn:
    conn.execute(delete(joconde_table))
    logging.info("🗑️  Table vidée")
    outils.chronometre_logging_lambda("Import PostgreSQL", lambda: conn.execute(insert(joconde_table), records))

logging.info("✅ Données importées")

# Vérification
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM joconde"))
    count = result.scalar()
    logging.info(f"🔍 Vérification : {count} lignes dans la table PostgreSQL")