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

print(f"✅ {locale.format_string('%d', len(df), grouping=True)} lignes chargées\n")

# ============================================================================
# ANALYSE 1 : Nombre d'œuvres par région
# ============================================================================
print("="*80)
print("📍 NOMBRE D'ŒUVRES PAR RÉGION")
print("="*80)
analyse_regions = df.group_by("region").agg(
    pl.len().alias("nombre_oeuvres")
).sort("nombre_oeuvres", descending=True)

print(analyse_regions.head(15))
print(f"\nTotal : {len(analyse_regions)} régions")

# ============================================================================
# ANALYSE 2 : Diversité des types d'objets par musée
# ============================================================================
print("\n" + "="*80)
print("🏛️  DIVERSITÉ DES TYPES D'OBJETS PAR MUSÉE")
print("="*80)
analyse_musees = df.group_by("nom_officiel_musee").agg(
    pl.col("denomination").n_unique().alias("types_objets"),
    pl.len().alias("nombre_oeuvres")
).sort("types_objets", descending=True)

print(analyse_musees.head(15))
print(f"\nTotal : {len(analyse_musees)} musées")

# ============================================================================
# ANALYSE 3 : Présence d'images par département
# ============================================================================
print("\n" + "="*80)
print("🖼️  PRÉSENCE D'IMAGES PAR DÉPARTEMENT")
print("="*80)
analyse_images = df.group_by("departement").agg([
    (pl.col("presence_image") == "oui").sum().alias("avec_image"),
    (pl.col("presence_image") != "oui").sum().alias("sans_image"),
    pl.len().alias("total")
]).with_columns([
    ((pl.col("avec_image") / pl.col("total")) * 100).round(2).alias("taux_image_%")
]).sort("avec_image", descending=True)

print(analyse_images.head(20))

# Statistiques globales
total_avec_image = analyse_images["avec_image"].sum()
total_sans_image = analyse_images["sans_image"].sum()
taux_global = (total_avec_image / (total_avec_image + total_sans_image)) * 100

print(f"\n📊 Statistiques globales :")
print(f"  • Œuvres avec image : {locale.format_string('%d', total_avec_image, grouping=True)} ({taux_global:.2f}%)")
print(f"  • Œuvres sans image : {locale.format_string('%d', total_sans_image, grouping=True)} ({100-taux_global:.2f}%)")

# ============================================================================
# ANALYSE 4 : Longueur moyenne des descriptions par région
# ============================================================================
print("\n" + "="*80)
print("📝 LONGUEUR MOYENNE DES DESCRIPTIONS PAR RÉGION")
print("="*80)
analyse_descriptions = df.group_by("region").agg([
    pl.col("description").str.len_chars().mean().alias("moyenne_longueur"),
    pl.col("description").str.len_chars().min().alias("min_longueur"),
    pl.col("description").str.len_chars().max().alias("max_longueur"),
    pl.len().alias("nombre_oeuvres")
]).sort("moyenne_longueur", descending=True)

print(analyse_descriptions.head(15))

# ============================================================================
# ANALYSES SUPPLÉMENTAIRES
# ============================================================================
print("\n" + "="*80)
print("📊 ANALYSES COMPLÉMENTAIRES")
print("="*80)

# Top 10 des auteurs les plus représentés
print("\n🎨 Top 10 des auteurs :")
top_auteurs = df.group_by("auteur").agg(
    pl.len().alias("nombre_oeuvres")
).sort("nombre_oeuvres", descending=True).head(10)
print(top_auteurs)

# Top 10 des villes
print("\n🏙️  Top 10 des villes :")
top_villes = df.group_by("ville").agg(
    pl.len().alias("nombre_oeuvres")
).sort("nombre_oeuvres", descending=True).head(10)
print(top_villes)

# Répartition par dénomination
print("\n🏺 Top 10 des dénominations :")
top_denominations = df.group_by("denomination").agg(
    pl.len().alias("nombre_oeuvres")
).sort("nombre_oeuvres", descending=True).head(10)
print(top_denominations)

# ============================================================================
# EXPORT DES RÉSULTATS
# ============================================================================
print("\n" + "="*80)
print("💾 EXPORT DES ANALYSES")
print("="*80)

# Export CSV
fichiers_csv = {
    "analyse_regions.csv": analyse_regions,
    "analyse_musees.csv": analyse_musees,
    "analyse_images.csv": analyse_images,
    "analyse_descriptions.csv": analyse_descriptions,
    "top_auteurs.csv": top_auteurs,
    "top_villes.csv": top_villes,
    "top_denominations.csv": top_denominations,
}

for nom_fichier, dataframe in fichiers_csv.items():
    chemin = f"/Users/macbook/Downloads/{nom_fichier}"
    dataframe.write_csv(chemin)
    print(f"✅ {nom_fichier}")

# ============================================================================
# RAPPORT JSON
# ============================================================================
rapport = {
    "date_generation": str(datetime.now(timezone.utc)),
    "source": fichier,
    "total_lignes": len(df),
    "statistiques": {
        "regions": {
            "nombre_regions": len(analyse_regions),
            "top_3": analyse_regions.head(3).to_dicts()
        },
        "musees": {
            "nombre_musees": len(analyse_musees),
            "top_3": analyse_musees.head(3).to_dicts()
        },
        "images": {
            "total_avec_image": int(total_avec_image),
            "total_sans_image": int(total_sans_image),
            "taux_global_pct": round(float(taux_global), 2)
        }
    }
}

rapport_path = "/Users/macbook/Downloads/rapport_analyses_joconde.json"
with open(rapport_path, "w", encoding="utf-8") as f:
    json.dump(rapport, f, indent=2, ensure_ascii=False)

print(f"✅ rapport_analyses_joconde.json")

print("\n🎉 Analyses terminées !")
print(f"\n📁 Fichiers générés dans : /Users/macbook/Downloads/")