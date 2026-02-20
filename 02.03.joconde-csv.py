import polars as pl
import outils

@outils.chronometre
def charger_fichier(path):
    return pl.read_csv(
        path,
        separator=";",
        schema_overrides={
            "Date_entree_dans_le_domaine_public": pl.Utf8,
            "Date_de_depot": pl.Utf8,   # 👈 AJOUT IMPORTANT
        }
    )

fichier = '/Users/macbook/Downloads/base-joconde-extrait.csv'
df = charger_fichier(fichier)

print(f"Mémoire utilisée : {round(df.estimated_size(unit='b') / (1024**2), 2)} Mo")
print(df.head(5))

df = df.select(["Reference", "Appellation", "Ville"])
print(df.columns)
