import polars as pl
import logging, yaml, os, locale, json

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

# Extraire la liste des régions uniques (triées, sans nulls)
print("\n🔍 Extraction des régions uniques...")

regions = (
    df.group_by("region")
      .len()
      .sort("region")
      .filter(pl.col("region").is_not_null())  # Exclure les valeurs nulles
      .select("region")
      .to_series()
      .to_list()
)

print(f"📍 {len(regions)} régions uniques trouvées")

# Afficher les régions
print("\n📋 Liste des régions :")
for i, region in enumerate(regions, 1):
    print(f"  {i:2d}. {region}")

# Sauvegarder dans config_regions.json
config_path = "config_regions.json"
with open(config_path, "w", encoding="utf-8") as f:
    json.dump({"regions": regions}, f, ensure_ascii=False, indent=2)

print(f"\n✅ Fichier créé : {config_path}")

# Afficher un aperçu du fichier JSON créé
print(f"\n📄 Contenu de {config_path} :")
with open(config_path, "r", encoding="utf-8") as f:
    content = json.load(f)
    print(json.dumps(content, ensure_ascii=False, indent=2))

print("\n🎉 Extraction terminée !")