import os
import yaml

from .utils.spark_session import get_spark_session
from .bronze.ingestion import ingest_all_boutiques
from .silver.cleaning import build_silver_table
from .gold.aggregation import build_gold_tables

def load_config() -> dict:
    base_dir = os.path.dirname(__file__)
    config_path = os.path.join(base_dir, "config", "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main(dbutils=None):
    config = load_config()
    spark = get_spark_session("SalesPipeline")
    ingest_all_boutiques(spark, dbutils, config)
    build_silver_table(spark, config)
    build_gold_tables(spark, config)

if __name__ == "__main__":
    main()