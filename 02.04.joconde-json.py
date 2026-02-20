import polars as pl
import outils

@outils.chronometre
def charger_fichier(path):
    """Charge un fichier JSON avec Polars"""
    return pl.read_json(path, infer_schema_length=None)

fichier = '/Users/macbook/Downloads/base-joconde-extrait.json'
df = charger_fichier(fichier)

# Convertir uniquement les colonnes qui ne sont pas des listes
for col in df.columns:
    dtype = df[col].dtype
    # Si ce n'est pas déjà une liste, convertir en Utf8
    if not isinstance(dtype, pl.List):
        df = df.with_columns(pl.col(col).cast(pl.Utf8))

print(f"Mémoire utilisée : {round(df.estimated_size(unit='b') / (1024**2), 2)} Mo")
print(df.head(5))

df = df.select(["reference", "appellation", "ville"])
print(df.columns)