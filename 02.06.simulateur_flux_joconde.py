import json
import os
import time
from pathlib import Path
from datetime import datetime
import yaml

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Configuration
fichier = '/Users/macbook/Downloads/base-joconde-extrait.json'  # Chemin macOS
BATCH_SIZE = 10          # nombre d'enregistrements par fichier
INTERVAL_SECONDS = 5     # délai entre chaque dépôt de fichier

# Préparer le répertoire de sortie
os.makedirs(config["watchdog"]["input_directory"], exist_ok=True)

# Charger les données sources
print(f"📖 Chargement du fichier : {fichier}")
with open(fichier, encoding="utf-8") as f:
    data = json.load(f)

print(f"📊 {len(data)} enregistrements chargés")
print(f"📦 Création de {len(data) // BATCH_SIZE + 1} fichiers batch")
print(f"⏱️  Intervalle entre fichiers : {INTERVAL_SECONDS} secondes")
print("-" * 60)

# Découpe en lots et export
for i in range(0, len(data), BATCH_SIZE):
    batch = data[i:i+BATCH_SIZE]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_num = i // BATCH_SIZE + 1
    filename = f"joconde_batch_{batch_num:05d}_{timestamp}.json"
    filepath = Path(config["watchdog"]["input_directory"]) / filename
    
    with open(filepath, "w", encoding="utf-8") as f_out:
        json.dump(batch, f_out, ensure_ascii=False, indent=2)
    
    print(f"✅ Batch {batch_num}: {filepath.name} ({len(batch)} notices)")
    
    # Ne pas attendre après le dernier fichier
    if i + BATCH_SIZE < len(data):
        time.sleep(INTERVAL_SECONDS)

print("-" * 60)
print(f"✅ Terminé ! {len(data)} enregistrements répartis en {batch_num} fichiers")