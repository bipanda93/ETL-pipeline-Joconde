# Pipeline ETL Joconde - Modern Data Stack

Pipeline ETL automatisé pour le catalogue national des œuvres d'art des musées français.

## Description

Pipeline ETL qui collecte, transforme et charge les données du catalogue Joconde.

**Volume :** 721 629 œuvres d'art  
**Qualité :** 90.6% (653 686 enregistrements)  
**Stack :** Dagster + dbt + PostgreSQL + Polars

## Architecture

- **Orchestration** : Dagster
- **Transformation** : dbt  
- **Stockage** : PostgreSQL
- **Processing** : Polars

## Flux
```
API Joconde → Extract → Staging (721K) → Transform (dbt) → Production (653K)
```

## Métriques

| Métrique | Valeur |
|----------|--------|
| Œuvres | 721 629 |
| Production | 653 686 |
| Qualité | 90.6% |
| Optimisation | -90% |

## Auteur

**Franck Ulrich BIPANDA**  
Master 2 Data Engineer

- [LinkedIn](https://www.linkedin.com/in/franck-bipanda-13392372)
- [GitHub](https://github.com/bipanda93)
