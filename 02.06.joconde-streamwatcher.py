import polars as pl
import outils
from sqlalchemy import MetaData, Table, Column, String, Text, create_engine, insert, text
from dotenv import load_dotenv
import logging, os, shutil, time, json
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

# Créer la base de données si elle n'existe pas
admin_engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5434')}/"
    f"airflow"
)

with admin_engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT")
    result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = 'joconde'"))
    if not result.fetchone():
        logging.info("🔨 Création de la base de données 'joconde'")
        conn.execute(text("CREATE DATABASE joconde"))
        logging.info("✅ Base de données 'joconde' créée")

admin_engine.dispose()

# Connexion PostgreSQL
engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5434')}/"
    f"{os.getenv('POSTGRES_DB', 'joconde')}"
)

metadata = MetaData()

joconde_table = Table(
    "joconde", 
    metadata,
    Column("reference", String),
    Column("appellation", String),
    Column("auteur", String),
    Column("date_creation", String),
    Column("denomination", String),
    Column("region", String),
    Column("departement", String),
    Column("ville", String),
    Column("description", Text),
)

metadata.create_all(engine)
logging.info("✅ Table 'joconde' prête")

# --- Traitement d'un fichier JSON ---
def traiter_fichier(filepath):
    logging.info(f"📥 Nouveau fichier détecté : {filepath}")
    try:
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
        
        with engine.begin() as conn:
            for i, record in enumerate(data):
                try:
                    row = {
                        "reference": record.get("reference"),
                        "appellation": record.get("appellation"),
                        "auteur": record.get("auteur"),
                        "date_creation": record.get("date_creation"),
                        "denomination": record.get("denomination"),
                        "region": record.get("region"),
                        "departement": record.get("departement"),
                        "ville": record.get("ville"),
                        "description": record.get("description"),
                    }
                    conn.execute(insert(joconde_table).values(**row))
                    
                    if (i + 1) % 1000 == 0:
                        logging.info(f"✅ {i+1}/{len(data)} lignes insérées")
                        
                except Exception as e:
                    logging.exception(f"❌ Erreur ligne {i+1}: {e}")
        
        logging.info(f"✅ {len(data)} lignes insérées au total")
        
        # Déplacer le fichier une fois traité
        archive_path = os.path.join(config["watchdog"]["archive_directory"], os.path.basename(filepath))
        shutil.move(filepath, archive_path)
        logging.info(f"📦 Fichier archivé : {archive_path}")
        
    except Exception as e:
        logging.exception(f"❌ Erreur de traitement : {e}")

# --- Gestionnaire d'événements ---
class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".json"):
            return
        time.sleep(1)  # attendre que le fichier soit totalement écrit
        traiter_fichier(event.src_path)

# --- Mise en place de l'observateur ---
if __name__ == "__main__":
    os.makedirs(config["watchdog"]["input_directory"], exist_ok=True)
    os.makedirs(config["watchdog"]["archive_directory"], exist_ok=True)
    
    observer = Observer()
    observer.schedule(Handler(), path=config["watchdog"]["input_directory"], recursive=False)
    observer.start()
    
    logging.info(f"👀 Surveillance du dossier '{config['watchdog']['input_directory']}' pour les nouveaux fichiers JSON...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("🛑 Arrêt de la surveillance...")
        observer.stop()
    
    observer.join()
    logging.info("✅ Programme terminé")