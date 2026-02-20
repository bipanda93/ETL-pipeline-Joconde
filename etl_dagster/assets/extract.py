from dagster import asset
import polars as pl
import os
import sys
sys.path.append('..')
from utils import get_config

@asset
def donnees_brutes() -> pl.DataFrame:
    config = get_config()
    source = config["fichiers"]["source"]
    cache = config["fichiers"]["cache"]
    
    if os.path.exists(cache):
        return pl.read_ipc(cache)
    else:
        df = pl.read_json(source, infer_schema_length=None)
        df.write_ipc(cache)
        return df
