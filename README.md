# 🎨 Pipeline ETL Modern Data Stack - Base Joconde

> Pipeline ETL automatisé traitant 721,629 œuvres d'art du catalogue national des musées français

[![Python](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue.svg)](https://www.postgresql.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11-orange.svg)](https://www.getdbt.com/)
[![Dagster](https://img.shields.io/badge/Dagster-1.11-purple.svg)](https://dagster.io/)

## 📋 Vue d'ensemble

Pipeline ETL moderne démontrant les meilleures pratiques de data engineering : orchestration Dagster, transformations dbt avec tests qualité, architecture staging-production PostgreSQL, et documentation automatique.

### 🎯 Résultats

| Métrique | Valeur |
|----------|--------|
| **Enregistrements source** | 721,629 |
| **Enregistrements production** | 653,686 (90.6%) |
| **Optimisation temps** | -90% |
| **Compression données** | 10x (450MB → 45MB) |
| **Tests qualité** | 2 tests dbt (100% pass) |

## 🏗️ Architecture
```
JSON Source (450 MB)
    ↓
Cache Feather (performance)
    ↓
staging.joconde (PostgreSQL) - 721,629 lignes
    ↓
dbt transformations + tests qualité
    ↓
joconde_oeuvre (production) - 653,686 lignes
    ↓
Tables temporelles (audit trail)
```

## 🛠️ Stack Technique

- **Orchestration :** Dagster, Prefect
- **Transformation :** dbt Core + dbt-postgres
- **Data Processing :** Python, Polars, SQLAlchemy
- **Database :** PostgreSQL 13 (Docker)
- **Tools :** pgAdmin, Git, Docker

## 🚀 Quick Start

### Prérequis
```bash
# Python 3.13+
python --version

# Docker Desktop
docker --version

# PostgreSQL client
psql --version
```

### Installation
```bash
# Cloner le repo
git clone https://github.com/bipanda93/etl-pipeline-joconde.git
cd etl-pipeline-joconde

# Créer l'environnement virtuel
python -m venv .venv
source .venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt

# Démarrer PostgreSQL
docker-compose up -d

# Créer les tables
docker exec -i postgres-1 psql -U airflow -d joconde_staging < sql/create_production.sql
```

### Exécution
```bash
# Avec Dagster (recommandé)
dagster dev -f etl_dagster/definitions.py -d etl_dagster
# Ouvrir http://localhost:3000

# Avec dbt
cd dbt/joconde
dbt run
dbt test
dbt docs serve  # Documentation sur http://localhost:8080
```

## 📂 Structure
```
etl-pipeline-joconde/
├── config.yaml                    # Configuration centrale
├── docker-compose.yml             # Infrastructure Docker
├── requirements.txt               # Dépendances Python
│
├── sql/                           # Scripts SQL
│   ├── create_production.sql     # Tables production
│   ├── importation.sql           # Chargement
│   └── analyses.sql              # Requêtes
│
├── etl_dagster/                  # Pipeline Dagster
│   ├── definitions.py
│   ├── utils.py
│   └── assets/
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       └── dbt_assets.py
│
└── dbt/joconde/                  # Projet dbt
    ├── models/
    │   ├── joconde_cleaned.sql
    │   └── schema.yml
    └── target/
        └── manifest.json
```

## 🧪 Tests & Qualité
```bash
# Tests dbt
dbt test

# Vérification données
docker exec -it postgres-1 psql -U airflow -d joconde_staging \
  -c "SELECT COUNT(*) FROM joconde_oeuvre;"
```

## 📊 Compétences démontrées

- Pipeline ETL/ELT moderne
- Orchestration workflow (Dagster)
- Data transformation (dbt)
- Tests qualité automatisés
- Optimisation performances
- Infrastructure as Code
- Documentation as Code

## 📄 Licence

MIT License

## 👤 Auteur

**Franck Bipanda**  
Data Engineer | Master Data Engineer @ F2I Institut

- 🔗 LinkedIn: [franck-bipanda](https://www.linkedin.com/in/franck-bipanda-13392372)
- 🌐 Portfolio: [datascienceportfol.io/bipandaf](https://www.datascienceportfol.io/bipandaf)
- 📧 Email: bipanda.franck@icloud.com

---

⭐ Si ce projet vous a aidé, n'hésitez pas à lui donner une étoile !
