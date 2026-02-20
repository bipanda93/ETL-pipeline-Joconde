import polars as pl
import os

def taille_fichier(path):
    """Retourne la taille d'un fichier en Mo"""
    taille_octets = os.path.getsize(path)
    taille_mo = taille_octets / (1024 * 1024)
    return round(taille_mo, 2)

# Chemins macOS
fichier_json = '/Users/macbook/Downloads/base-joconde-extrait.json'
fichier_parquet = '/Users/macbook/Downloads/joconde.parquet'

print("📖 Chargement du fichier JSON...")
df = pl.read_json(
    fichier_json, 
    infer_schema_length=None  # Scanner tout le fichier pour éviter les erreurs
)

print(f"📊 {len(df)} lignes chargées")
print(f"📋 Colonnes disponibles : {len(df.columns)} colonnes")

# Sélectionner les colonnes
df = df.select([
    "reference", "appellation", "auteur",
    "date_creation", "denomination", "region",
    "departement", "ville", "description"
])

print(f"✂️  Colonnes sélectionnées : {df.columns}")

print("💾 Écriture du fichier Parquet...")
df.write_parquet(fichier_parquet, compression="snappy")

print("✅ Vérification : rechargement du fichier Parquet")
df_recharge = pl.read_parquet(fichier_parquet)
print(df_recharge.head())

print("\n" + "="*60)
print(f"📄 Taille du fichier JSON     : {taille_fichier(fichier_json)} Mo")
print(f"📦 Taille du fichier Parquet  : {taille_fichier(fichier_parquet)} Mo")
ratio = round(taille_fichier(fichier_json) / taille_fichier(fichier_parquet), 2)
print(f"🚀 Gain de compression        : {ratio}x plus petit")
print(f"💾 Espace économisé           : {taille_fichier(fichier_json) - taille_fichier(fichier_parquet)} Mo")
print("="*60)