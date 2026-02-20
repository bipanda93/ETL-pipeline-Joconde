import polars as pl
import logging, yaml, os, locale, json
from datetime import datetime

# Gestion robuste de la locale
try:
    locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'fr_FR')
    except locale.Error:
        print("⚠️  Locale français non disponible, utilisation de la locale par défaut")
        locale.setlocale(locale.LC_ALL, '')

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Chemins macOS
fichier = '/Users/macbook/Downloads/base-joconde-extrait.json'
fichier_cache = "/Users/macbook/Downloads/joconde_cache.feather"

# Chargement avec cache
if os.path.exists(fichier_cache):
    print("📦 Chargement depuis le cache feather...")
    df = pl.read_ipc(fichier_cache)
else:
    print("📥 Chargement depuis le JSON source...")
    df = pl.read_json(fichier, infer_schema_length=None)
    
    # Sauvegarde rapide en cache
    print("💾 Sauvegarde du cache feather...")
    df.write_ipc(fichier_cache)

print(f"✅ {locale.format_string('%d', len(df), grouping=True)} lignes chargées")

# Charger la configuration des régions
with open("config_regions.json", encoding="utf-8") as f:
    config_regions = json.load(f)
    regions_valides = config_regions["regions"]

print(f"\n📍 {len(regions_valides)} régions valides chargées :")
print(regions_valides)

annee_courante = datetime.now().year
print(f"\n📅 Année courante : {annee_courante}")

print("\n🔍 Analyse des anomalies...")

# Ajout de colonnes booléennes pour repérer les anomalies
df = df.with_columns([
    (pl.col("reference").is_null()).alias("anomalie_reference_absente"),
    (pl.col("date_creation")
     .cast(pl.Int64, strict=False)
     .is_between(-3000, annee_courante)
     .not_()).alias("anomalie_date_creation"),
    (pl.col("region").is_in(regions_valides).not_()).alias("anomalie_region_inconnue"),
    (pl.col("description").str.len_chars() < 10).alias("anomalie_description_courte")
])

# Statistiques des anomalies
print("\n" + "="*60)
print("📊 RAPPORT DES ANOMALIES DÉTECTÉES")
print("="*60)

anomalies = df.select([
    pl.sum("anomalie_reference_absente").alias("références_manquantes"),
    pl.sum("anomalie_date_creation").alias("dates_incohérentes"),
    pl.sum("anomalie_region_inconnue").alias("régions_inconnues"),
    pl.sum("anomalie_description_courte").alias("descriptions_trop_courtes"),
])

print(anomalies)

# Affichage avec pourcentages
total_lignes = len(df)
print("\n📈 Détail des anomalies (avec pourcentages) :")
for col in anomalies.columns:
    count = anomalies[col][0]
    pct = (count / total_lignes) * 100
    print(f"  • {col.replace('_', ' ').capitalize():30} : {locale.format_string('%d', count, grouping=True):>10} ({pct:>5.2f}%)")

# Exemples d'anomalies
print("\n" + "="*60)
print("🔎 EXEMPLES D'ANOMALIES")
print("="*60)

# Références absentes
if anomalies["références_manquantes"][0] > 0:
    print("\n❌ Exemples de références manquantes :")
    print(df.filter(pl.col("anomalie_reference_absente"))
          .select(["reference", "appellation", "ville"])
          .head(5))

# Dates incohérentes
if anomalies["dates_incohérentes"][0] > 0:
    print("\n📅 Exemples de dates incohérentes :")
    print(df.filter(pl.col("anomalie_date_creation"))
          .select(["reference", "date_creation", "appellation"])
          .head(5))

# Régions inconnues
if anomalies["régions_inconnues"][0] > 0:
    print("\n📍 Exemples de régions inconnues :")
    regions_non_valides = (
        df.filter(pl.col("anomalie_region_inconnue"))
        .select("region")
        .unique()
        .sort("region")
    )
    print(f"   {len(regions_non_valides)} régions différentes non reconnues :")
    print(regions_non_valides.head(10))

# Descriptions courtes
if anomalies["descriptions_trop_courtes"][0] > 0:
    print("\n📝 Exemples de descriptions trop courtes :")
    print(df.filter(pl.col("anomalie_description_courte"))
          .select(["reference", "description", "appellation"])
          .head(5))

# Score de qualité global
total_anomalies = sum([anomalies[col][0] for col in anomalies.columns])
score_qualite = 100 - (total_anomalies / (total_lignes * 4)) * 100

print("\n" + "="*60)
print(f"✅ SCORE DE QUALITÉ GLOBAL : {score_qualite:.2f}%")
print("="*60)