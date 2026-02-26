# 🎨 Pipeline ETL Modern Data Stack - Base Joconde

> Pipeline ETL automatisé traitant **721,629 œuvres d'art** du catalogue national des musées français

[![Python](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue.svg)](https://www.postgresql.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11-orange.svg)](https://www.getdbt.com/)
[![Dagster](https://img.shields.io/badge/Dagster-1.11-purple.svg)](https://dagster.io/)

## 📊 Résultats

| Métrique | Valeur |
|----------|--------|
| **Enregistrements traités** | 721,629 |
| **Enregistrements production** | 653,686 (90.6%) |
| **Optimisation temps** | -90% |
| **Compression données** | 10x (450MB → 45MB) |
| **Tests qualité** | 2/2 pass ✅ |

## 🎯 Objectif

Démonstration d'un pipeline ETL moderne avec :
- ✅ Architecture staging-production PostgreSQL
- ✅ Orchestration Dagster avec monitoring temps réel
- ✅ Transformations dbt avec tests qualité
- ✅ Infrastructure Docker reproductible
- ✅ Documentation automatique

## 🏗️ Architecture
```
JSON Source (450 MB)
    ↓
Cache Feather (-90% temps)
    ↓
staging.joconde (PostgreSQL) - 721,629 lignes
    ↓
dbt transformations + tests
    ↓
joconde_oeuvre (production) - 653,686 lignes
    ↓
Tables temporelles (audit)
```

## 🛠️ Stack Technique

**Orchestration :** Dagster, Prefect  
**Transformation :** dbt Core + dbt-postgres  
**Processing :** Python 3.13, Polars, SQLAlchemy  
**Database :** PostgreSQL 13 (Docker)  
**Tools :** pgAdmin, Git, Docker

## 🚀 Quick Start

### Installation
```bash
git clone https://github.com/bipanda93/ETL-pipeline-Joconde.git
cd ETL-pipeline-Joconde

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

docker-compose up -d
docker exec -i postgres-1 psql -U airflow -d joconde_staging < sql/create_production.sql
```

### Exécution

**Avec Dagster (recommandé) :**
```bash
dagster dev -f etl_dagster/definitions.py -d etl_dagster
```

**Avec dbt :**
```bash
cd dbt/joconde
dbt run
dbt test
dbt docs serve
```

## 📂 Structure
```
ETL-pipeline-Joconde/
├── sql/
├── etl_dagster/
│   ├── definitions.py
│   └── assets/
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       └── dbt_assets.py
├── dbt/joconde/
│   ├── models/
│   └── target/
└── requirements.txt
```

## 🧪 Tests
```bash
cd dbt/joconde
dbt test

docker exec -it postgres-1 psql -U airflow -d joconde_staging \
  -c "SELECT COUNT(*) FROM joconde_oeuvre;"
```

## 📊 Analyses
```sql
SELECT region, COUNT(*) as nb_oeuvres
FROM joconde_oeuvre
GROUP BY region
ORDER BY nb_oeuvres DESC
LIMIT 10;

SELECT * FROM v_oeuvres_par_siecle;
```

## 🎓 Compétences démontrées

**Data Engineering**
- Pipeline ETL/ELT moderne
- Orchestration (Dagster)
- Data Quality (dbt tests)
- Performance optimization

**Software Engineering**
- Clean code & architecture modulaire
- Documentation as code
- Infrastructure as Code

**DevOps**
- Docker containerization
- Configuration management
- Monitoring & observability

## 📚 Source des données

[Base Joconde (data.gouv.fr)](https://data.culture.gouv.fr/explore/dataset/base-joconde-extrait/)  
Licence : Licence Ouverte / Open License (Etalab)

## 👤 Auteur

**Franck Bipanda**  
*Data Engineer | Master Data Engineer @ F2I Institut*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Franck_Bipanda-blue?logo=linkedin)](https://www.linkedin.com/in/franck-bipanda-13392372)
[![Portfolio](https://img.shields.io/badge/Portfolio-orange)](https://www.datascienceportfol.io/bipandaf)
[![Email](https://img.shields.io/badge/Email-red?logo=gmail)](mailto:bipanda.franck@icloud.com)

---

⭐ **Recherche stage Data Engineer (6 mois)** | Python • SQL • Dagster • dbt • PostgreSQL

---

⭐ Si ce projet vous a aidé, donnez-lui une étoile !
