import polars as pl
from sqlalchemy import create_engine, text, Table, Column, MetaData, String, Text, DateTime, insert, delete
from datetime import datetime, timezone
import logging
import os

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

print("="*80)
print("🚀 CHARGEMENT JOCONDE VERS STAGING.JOCONDE")
print("="*80)

# ============================================================================
# CONFIGURATION POSTGRESQL
# ============================================================================
DB_CONFIG = {
    'user': 'joconde_import',
    'password': 'MsacR%GK85.VoykyEU',
    'host': 'localhost',
    'port': 5434,
    'database': 'joconde_staging',
    'schema': 'staging'
}

# Chemins
fichier_json = '/Users/macbook/Downloads/base-joconde-extrait.json'
fichier_cache = "/Users/macbook/Downloads/joconde_cache.feather"

# ============================================================================
# CONNEXION POSTGRESQL
# ============================================================================
print("\n📡 Connexion à PostgreSQL...")
engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
    executemany_mode='values_plus_batch'
)

# Test de connexion
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT current_database(), current_user;"))
        db, user = result.fetchone()
        print(f"✅ Connecté à '{db}' en tant que '{user}'")
except Exception as e:
    print(f"❌ Erreur de connexion : {e}")
    exit(1)

# ============================================================================
# CHARGEMENT DES DONNÉES
# ============================================================================
print("\n📥 Chargement des données Joconde...")

if os.path.exists(fichier_cache):
    print("📦 Chargement depuis le cache feather...")
    df = pl.read_ipc(fichier_cache)
else:
    print("📥 Chargement depuis le JSON source...")
    df = pl.read_json(fichier_json, infer_schema_length=None)
    print("💾 Sauvegarde du cache feather...")
    df.write_ipc(fichier_cache)

print(f"✅ {len(df):,} lignes chargées")

# ============================================================================
# PRÉPARATION DES DONNÉES
# ============================================================================
print("\n🔧 Préparation des données pour insertion...")

# Sélectionner et renommer les colonnes
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
    pl.lit("joconde_json").alias("source_system"),
    pl.lit("etl_batch_python_staging").alias("load_process")
])

# Convertir en liste de dictionnaires
records = df_staging.to_dicts()
print(f"✅ {len(records):,} enregistrements préparés")

# ============================================================================
# DÉFINITION DE LA TABLE SQLALCHEMY
# ============================================================================
metadata = MetaData(schema=DB_CONFIG['schema'])

table_joconde = Table(
    'joconde',
    metadata,
    Column('reference', String(50)),
    Column('appellation', String(255)),
    Column('auteur', String(255)),
    Column('date_creation', String(50)),
    Column('region', String(100)),
    Column('departement', String(100)),
    Column('description', Text),
    Column('load_timestamp_utc', DateTime(timezone=True)),
    Column('source_system', String(50)),
    Column('load_process', String(50)),
    schema=DB_CONFIG['schema']
)

# ============================================================================
# INSERTION EN BASE (PAR LOTS POUR PERFORMANCE)
# ============================================================================
print("\n💾 Insertion en base de données...")

BATCH_SIZE = 10000  # Insérer par lots de 10k lignes

try:
    with engine.begin() as conn:
        # Vider la table
        print("🗑️  Suppression des données existantes...")
        conn.execute(text(f"TRUNCATE TABLE {DB_CONFIG['schema']}.joconde"))
        
        # Insérer par lots
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"📝 Insertion de {len(records):,} lignes en {total_batches} lots...")
        
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            conn.execute(insert(table_joconde), batch)
            batch_num = (i // BATCH_SIZE) + 1
            print(f"   ✅ Lot {batch_num}/{total_batches} ({len(batch):,} lignes)")
        
    print("✅ Données insérées avec succès !")
    
except Exception as e:
    print(f"❌ Erreur lors de l'insertion : {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# ============================================================================
# VÉRIFICATION
# ============================================================================
print("\n🔍 Vérification des données...")

with engine.connect() as conn:
    # Compter les lignes
    result = conn.execute(text(f"SELECT COUNT(*) FROM {DB_CONFIG['schema']}.joconde"))
    count = result.fetchone()[0]
    print(f"✅ Nombre de lignes insérées : {count:,}")
    
    # Vérifier que le compte correspond
    if count == len(records):
        print("✅ Toutes les lignes ont été insérées correctement")
    else:
        print(f"⚠️  Attention : {len(records):,} lignes préparées mais {count:,} insérées")
    
    # Statistiques par région
    result = conn.execute(text(f"""
        SELECT region, COUNT(*) as nb
        FROM {DB_CONFIG['schema']}.joconde
        WHERE region IS NOT NULL
        GROUP BY region
        ORDER BY nb DESC
        LIMIT 5
    """))
    
    print("\n📊 Top 5 des régions :")
    for row in result:
        print(f"   {row[0]}: {row[1]:,} œuvres")
    
    # Exemple de données
    result = conn.execute(text(f"""
        SELECT reference, appellation, region, load_timestamp_utc
        FROM {DB_CONFIG['schema']}.joconde
        LIMIT 3
    """))
    
    print("\n📄 Exemple de données :")
    for row in result:
        print(f"   {row[0]} | {row[1][:50] if row[1] else 'N/A'}... | {row[2]}")

print("\n🎉 Chargement terminé avec succès !")
print("="*80)