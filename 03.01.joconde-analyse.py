import polars as pl
import logging, yaml, os, locale

# Définir le format local (français standard)
try:
    locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')  # macOS/Linux
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'fr_FR')  # Alternative macOS
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
    print(f"✅ Chargé en {os.path.getsize(fichier_cache) / (1024**2):.2f} Mo")
else:
    print("📥 Chargement depuis le JSON source...")
    df = pl.read_json(fichier, infer_schema_length=None)
    
    # Sauvegarde rapide en cache
    print("💾 Sauvegarde du cache feather...")
    df.write_ipc(fichier_cache)
    print(f"✅ Cache créé : {os.path.getsize(fichier_cache) / (1024**2):.2f} Mo")

print("\n" + "="*60)
print("📊 ANALYSE DES DONNÉES")
print("="*60)

# Compter les null par colonne
print("\n🔍 Nombre de valeurs NULL par colonne :")
null_counts = df.null_count()
print(null_counts)

# Afficher seulement les colonnes avec des nulls
print("\n⚠️  Colonnes avec des NULL :")
for col in df.columns:
    null_count = df[col].null_count()
    if null_count > 0:
        null_pct = (null_count / len(df)) * 100
        print(f"  - {col}: {locale.format_string('%d', null_count, grouping=True)} ({null_pct:.2f}%)")

# Supprimer les lignes où 'titre' est null
print("\n🗑️  Suppression des lignes sans titre...")
print(f"Avant : {locale.format_string('%d', df.height, grouping=True)} lignes")
df = df.drop_nulls(subset=["titre"])
print(f"Après : {locale.format_string('%d', df.height, grouping=True)} lignes")

# Remplir les valeurs nulles de 'epoque'
print("\n📝 Traitement de la colonne 'epoque'...")
print("Avant remplissage :")
print(df.select(["epoque"]).head(10))

df = df.with_columns(
    pl.col("epoque").fill_null("inconnu")
)

print("\nAprès remplissage :")
print(df.select(["epoque"]).head(10))

# Suppression des doublons
print("\n🔄 Suppression des doublons sur [titre, code_museofile]...")
print(f"Avant : {locale.format_string('%d', df.height, grouping=True)} lignes")

df = df.unique(subset=["titre", "code_museofile"], keep='first', maintain_order=True)

print(f"Après : {locale.format_string('%d', df.height, grouping=True)} lignes")
nb_doublons_supprimes = df.height - df.unique(subset=["titre", "code_museofile"], keep='first', maintain_order=True).height

print("\n" + "="*60)
print("📋 APERÇU DES DONNÉES NETTOYÉES")
print("="*60)
print(df.head(10))

print("\n✅ Nettoyage terminé !")