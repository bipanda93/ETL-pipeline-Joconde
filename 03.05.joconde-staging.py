import polars as pl
import outils
from sqlalchemy import MetaData, create_engine, insert, text, Table, Column, String, Text, DateTime
from datetime import datetime, timezone
import logging, os, yaml

# Configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    filename="staging.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================================================
# CONFIGURATION POSTGRESQL AVEC VOS IDENTIFIANTS
# ============================================================================
DB_CONFIG = {
    'user': 'joconde_import',
    'password': 'MsacR%GK85.VoykyEU',
    'host': 'localhost',
    'port': '5434',
    'database': 'joconde_staging',
    'schema': 'staging'
}

# Chemins macOS
fichier = '/Users/macbook/Downloads/base-joconde-extrait.json'
fichier_cache = "/Users/macbook/Downloads/joconde_cache.feather"

# ============================================================================
# CHARGEMENT DES DONNÉES
# ============================================================================
if os.path.exists(fichier_cache):
    print("📦 Chargement depuis le cache feather...")
    df = pl.read_ipc(fichier_cache)
else:
    print("📥 Chargement depuis le JSON source...")
    df = pl.read_json(fichier, infer_schema_length=None)
    df.write_ipc(fichier_cache)

print(f"✅ {len(df):,} lignes chargées")

# ============================================================================
# CONNEXION POSTGRESQL
# ============================================================================
engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
    executemany_mode='values_plus_batch'
)

print("✅ Connexion PostgreSQL établie")

# ============================================================================
# DÉFINITION DE LA TABLE
# ============================================================================
metadata = MetaData(schema=DB_CONFIG['schema'])

# Extraire le nom de la table depuis config (ex: "staging.joconde" -> "joconde")
table_name = config["staging"]["table"].split('.')[-1] if '.' in config["staging"]["table"] else config["staging"]["table"]

joconde_table = Table(
    table_name,
    metadata,
    Column('reference', Text),
    Column('appellation', Text),
    Column('auteur', Text),
    Column('date_creation', Text),
    Column('region', Text),
    Column('departement', Text),
    Column('description', Text),
    Column('load_timestamp_utc', DateTime(timezone=True)),
    Column('source_system', String(50)),
    Column('load_process', String(50)),
    schema=DB_CONFIG['schema']
)

# ============================================================================
# PRÉPARATION DES DONNÉES
# ============================================================================
print("🔧 Préparation des données...")

df_staging = df.select([
    pl.col("reference"),
    pl.col("appellation"),
    pl.col("auteur"),
    pl.col("date_creation"),
    pl.col("region"),
    pl.col("departement"),
    pl.col("description")
]).with_columns([
    pl.lit(datetime.now(timezone.utc)).alias("load_timestamp_utc"),
    pl.lit(config["audit"]["source_system"]).alias("source_system"),
    pl.lit(config["audit"]["load_process"]).alias("load_process")
])

records = df_staging.to_dicts()
print(f"✅ {len(records):,} enregistrements préparés")

# ============================================================================
# INSERTION EN BASE
# ============================================================================
print("💾 Insertion en base de données...")

BATCH_SIZE = 10000

with engine.begin() as conn:
    # Vider la table
    print("🗑️  Suppression des données existantes...")
    conn.execute(text(f"TRUNCATE TABLE {DB_CONFIG['schema']}.{table_name}"))
    
    # Insertion par lots avec chronomètre
    def insert_data():
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"📝 Insertion de {len(records):,} lignes en {total_batches} lots...")
        
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            conn.execute(insert(joconde_table), batch)
            batch_num = (i // BATCH_SIZE) + 1
            
            if batch_num % 10 == 0:  # Afficher tous les 10 lots
                print(f"   ✅ Lot {batch_num}/{total_batches}")
    
    outils.chronometre_logging_lambda("Import PostgreSQL", insert_data)

logging.info(f"Données importées : {len(records):,} lignes")
print("✅ Import terminé avec succès !")

# ============================================================================
# VÉRIFICATION
# ============================================================================
print("\n🔍 Vérification des données...")

with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM {DB_CONFIG['schema']}.{table_name}"))
    count = result.fetchone()[0]
    print(f"✅ Nombre de lignes en base : {count:,}")
    logging.info(f"Vérification : {count:,} lignes en base")
    
    # Vérifier que le compte correspond
    if count == len(records):
        print("✅ Toutes les lignes ont été insérées correctement")
    else:
        print(f"⚠️  Attention : {len(records):,} lignes préparées mais {count:,} insérées")