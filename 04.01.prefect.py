from prefect import flow, task
import polars as pl
from dotenv import load_dotenv
import logging, os
import yaml
from datetime import datetime, timezone
from sqlalchemy import MetaData, create_engine, text, insert

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

fichier_cache = config["fichiers"]["cache"]

@task
def extract_json(fichier: str) -> pl.DataFrame:
    if os.path.exists(fichier_cache):
        df = pl.read_ipc(fichier_cache)
    else:
        df = pl.read_json(fichier, infer_schema_length=None)
        df.write_ipc(fichier_cache)
    return df

@task
def transform_data(df: pl.DataFrame) -> pl.DataFrame:
    df_clean = df.with_columns([
        pl.col("date_creation").str.extract(r"(\d{4})", 1).cast(pl.Int64, strict=False).alias("annee_creation"),
        pl.col("region").str.to_titlecase().alias("region_normalisee"),
        pl.when(pl.col("description").str.len_chars() > 200)
        .then(pl.col("description").str.slice(0, 200) + "...")
        .otherwise(pl.col("description"))
        .alias("description_resumee"),
        pl.col("artiste_sous_droits").is_not_null().alias("artiste_protégé"),
        pl.lit(datetime.now(timezone.utc).date()).alias("date_import")
    ])
    return df_clean

@task
def load_to_postgresql(df: pl.DataFrame):
    DB_CONFIG = {
        'user': 'joconde_import',
        'password': 'MsacR%GK85.VoykyEU',
        'host': 'localhost',
        'port': '5434',
        'database': 'joconde_staging',
        'schema': 'staging'
    }
    
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
        executemany_mode='values_plus_batch'
    )
    
    metadata = MetaData(schema=DB_CONFIG['schema'])
    metadata.reflect(bind=engine)
    joconde_table = metadata.tables[config["staging"]["table"]]
    
    records = [
        {
            **row,
            "source_system": config["audit"]["source_system"],
            "load_process": config["audit"]["load_process"]
        }
        for row in df.to_dicts()
    ]
    
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {config['staging']['table']};"))
        conn.execute(insert(joconde_table), records)

@flow
def etl_flow(fichier_source: str):
    df_raw = extract_json(fichier_source)
    df_clean = transform_data(df_raw)
    load_to_postgresql(df_clean)

if __name__ == "__main__":
    fichier_source = config["fichiers"]["source"]
    etl_flow(fichier_source)