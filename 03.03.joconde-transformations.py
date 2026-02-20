import polars as pl
import logging, yaml, os, locale, json
from datetime import datetime, timezone

# Gestion robuste de la locale
try:
    locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'fr_FR')
    except locale.Error:
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

print("\n📋 Colonnes disponibles :")
print(df.columns)

print("\n🔧 Application des transformations...")

# Transformations et enrichissements
df = df.with_columns([
    # Extraire l'année comme entier à partir de la date de création
    pl.col("date_creation")
      .str.extract(r"(\d{4})", 1)
      .cast(pl.Int64, strict=False)
      .alias("annee_creation"),
    
    # Normaliser les noms de région : capitaliser la première lettre
    pl.col("region")
      .str.to_titlecase()
      .alias("region_normalisee"),
    
    # Raccourcir les descriptions trop longues
    pl.when(pl.col("description").str.len_chars() > 200)
      .then(pl.col("description").str.slice(0, 200) + "...")
      .otherwise(pl.col("description"))
      .alias("description_resumee"),
    
    # Marquer les œuvres avec artiste sous droits ou non
    pl.col("artiste_sous_droits")
      .is_not_null()
      .alias("artiste_protégé"),
    
    # Ajouter une date d'import (UTC)
    pl.lit(datetime.now(timezone.utc).date())
      .alias("date_import")
])

print("\n" + "="*80)
print("📊 APERÇU DES TRANSFORMATIONS")
print("="*80)

# Afficher les résultats
print(df.select([
    "date_creation",
    "annee_creation", 
    "region", 
    "region_normalisee",
    "description_resumee", 
    "artiste_protégé", 
    "date_import"
]).head(10))

# Statistiques sur les transformations
print("\n" + "="*80)
print("📈 STATISTIQUES DES TRANSFORMATIONS")
print("="*80)

# Années extraites
annees_extraites = df.filter(pl.col("annee_creation").is_not_null()).height
print(f"\n📅 Années extraites :")
print(f"  • {locale.format_string('%d', annees_extraites, grouping=True)} dates converties avec succès")
print(f"  • {locale.format_string('%d', len(df) - annees_extraites, grouping=True)} dates non converties")

# Distribution des années
if annees_extraites > 0:
    print(f"\n📊 Distribution des années :")
    print(df.select([
        pl.col("annee_creation").min().alias("année_min"),
        pl.col("annee_creation").max().alias("année_max"),
        pl.col("annee_creation").mean().alias("année_moyenne"),
        pl.col("annee_creation").median().alias("année_médiane"),
    ]))

# Régions normalisées
regions_uniques = df.select("region_normalisee").unique().filter(
    pl.col("region_normalisee").is_not_null()
).height
print(f"\n📍 Régions normalisées :")
print(f"  • {regions_uniques} régions distinctes après normalisation")

# Top 10 des régions
print("\n🏆 Top 10 des régions :")
print(df.group_by("region_normalisee")
      .agg(pl.count().alias("nombre"))
      .sort("nombre", descending=True)
      .head(10))

# Descriptions raccourcies
descriptions_raccourcies = df.filter(
    pl.col("description").str.len_chars() > 200
).height
print(f"\n📝 Descriptions raccourcies :")
print(f"  • {locale.format_string('%d', descriptions_raccourcies, grouping=True)} descriptions tronquées (> 200 caractères)")

# Artistes sous droits
artistes_proteges = df.filter(pl.col("artiste_protégé") == True).height
print(f"\n©️  Artistes sous droits :")
print(f"  • {locale.format_string('%d', artistes_proteges, grouping=True)} œuvres d'artistes protégés")
print(f"  • {(artistes_proteges / len(df) * 100):.2f}% du total")

# Date d'import
print(f"\n📅 Date d'import ajoutée : {df['date_import'][0]}")

print("\n✅ Transformations terminées !")

# Export optionnel
export_fichier = "/Users/macbook/Downloads/joconde_enrichi.parquet"
print(f"\n💾 Export du DataFrame enrichi...")
df.write_parquet(export_fichier, compression="snappy")
print(f"✅ Fichier exporté : {export_fichier}")

# Afficher la taille des fichiers
taille_cache = os.path.getsize(fichier_cache) / (1024**2)
taille_export = os.path.getsize(export_fichier) / (1024**2)
print(f"\n📊 Tailles des fichiers :")
print(f"  • Cache Feather : {taille_cache:.2f} Mo")
print(f"  • Export Parquet : {taille_export:.2f} Mo")
print(f"  • Gain : {((taille_cache - taille_export) / taille_cache * 100):.2f}%")