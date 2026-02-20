import polars as pl

fichier_parquet = 'tomme-des-pyrenees.parquet'

# Charger et filtrer
df = pl.read_parquet(fichier_parquet)

# Chercher Camurac et afficher produit + code_insee
df_camurac = df.filter(
    pl.col("commune").str.to_lowercase() == "camurac"
).select(["produit", "code_insee"])

print(df_camurac)