import os
import yaml
import pytest

from sales_pipeline.utils.spark_session import get_spark_session
from sales_pipeline.bronze.ingestion import ingest_all_boutiques
from sales_pipeline.silver.cleaning import build_silver_table


def _get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception as e:
        raise RuntimeError(
            "Impossible d'initialiser DBUtils via pyspark.dbutils.DBUtils(spark). "
        ) from e


def test_pipeline_bronze_then_silver_has_expected_columns():
    spark = get_spark_session("TestPipeline")
    dbutils = _get_dbutils(spark)

    base_dir = os.path.dirname(os.path.dirname(__file__))
    cfg_path = os.path.join(base_dir, "sales_pipeline", "config", "config.yaml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    ingest_all_boutiques(spark, dbutils, config)
    df_silver = build_silver_table(spark, config)

    expected_cols = {
        "ID_Vente", "Date_Vente", "Nom_Produit", "Catégorie",
        "Prix_Unitaire", "Quantité", "Montant_Total",
        "Devise", "Nom_Boutique", "Ville", "Pays"
    }

    assert expected_cols.issubset(set(df_silver.columns)), (
        f"Colonnes manquantes : {expected_cols - set(df_silver.columns)}"
    )
