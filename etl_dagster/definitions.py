from dagster import Definitions
from dagster_dbt import DbtCliResource
from assets.extract import donnees_brutes
from assets.transform import donnees_transformees
from assets.load import chargement_postgresql
from assets.dbt_assets import dbt_models

defs = Definitions(
    assets=[
        donnees_brutes,
        donnees_transformees,
        chargement_postgresql,
        dbt_models
    ],
    resources={
        "dbt": DbtCliResource(project_dir="/Users/macbook/etl-python-sql-5256047/dbt/joconde")
    }
)
