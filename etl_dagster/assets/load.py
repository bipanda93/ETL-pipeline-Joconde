from dagster import asset
from sqlalchemy import MetaData, create_engine, text, insert
import sys
sys.path.append('..')
from utils import get_config

@asset
def chargement_postgresql(donnees_transformees):
    config = get_config()
    
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
    table = metadata.tables[config["staging"]["table"]]
    
    records = [
        {
            **row,
            "source_system": config["audit"]["source_system"],
            "load_process": config["audit"]["load_process"]
        }
        for row in donnees_transformees.to_dicts()
    ]
    
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {config['staging']['table']};"))
        conn.execute(insert(table), records)
