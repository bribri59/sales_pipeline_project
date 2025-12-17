from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
import os


def ingest_boutique(
    spark,
    dbutils,
    landing_path: str,
    archive_path: str,
    table_name: str,
    boutique_id: str,
    bronze_db: str,
) -> DataFrame:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")

    print(f"=== Ingestion Bronze pour la boutique : {boutique_id} ===")
    print(f"Lecture depuis : {landing_path}")

    files = [f.path for f in dbutils.fs.ls(landing_path) if f.path.lower().endswith(".csv")]

    if not files:
        print(f"Aucun fichier CSV pour {boutique_id}")
        return spark.createDataFrame([], "dummy string").limit(0)

    df = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(files)
             .withColumn("_ingestion_timestamp", current_timestamp())
             .withColumn("_source_file", col("_metadata.file_path"))
             .withColumn("_boutique_id", lit(boutique_id))
    )

    table_exists = spark.catalog.tableExists(table_name)

    writer = df.write.format("delta")
    if table_exists:
        writer.mode("append").saveAsTable(table_name)
    else:
        writer.mode("overwrite").saveAsTable(table_name)


    dbutils.fs.mkdirs(archive_path)
    for path in files:
        file_name = path.split("/")[-1]
        dest = f"{archive_path}/{file_name}"
        dbutils.fs.mv(path, dest)

    print(f"=== Fin ingestion Bronze pour {boutique_id} ===")
    return df


def ingest_all_boutiques(spark, dbutils, config: dict) -> None:
    """
    Appelle l’ingestion pour New York / Paris / Tokyo.
    """
    bronze_cfg = config["bronze"]
    bronze_db = bronze_cfg["db_name"]
    raw_root = bronze_cfg["raw_root"]
    archive_root = bronze_cfg["archive_root"]

    boutiques = [
        {
            "id": "new_york",
            "landing_path": os.path.join(raw_root, "boutique_new_york"),
            "archive_path": os.path.join(archive_root, "boutique_new_york"),
            "table_name": f"{bronze_db}.ventes_bronze_boutique_new_york",
        },
        {
            "id": "paris",
            "landing_path": os.path.join(raw_root, "boutique_paris"),
            "archive_path": os.path.join(archive_root, "boutique_paris"),
            "table_name": f"{bronze_db}.ventes_bronze_boutique_paris",
        },
        {
            "id": "tokyo",
            "landing_path": os.path.join(raw_root, "boutique_tokyo"),
            "archive_path": os.path.join(archive_root, "boutique_tokyo"),
            "table_name": f"{bronze_db}.ventes_bronze_boutique_tokyo",
        },
    ]

    for cfg in boutiques:
        ingest_boutique(
            spark,
            dbutils,
            cfg["landing_path"],
            cfg["archive_path"],
            cfg["table_name"],
            cfg["id"],
            bronze_db,
        )
