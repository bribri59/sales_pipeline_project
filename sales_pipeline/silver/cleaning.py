from pyspark.sql.functions import col, coalesce, lit, current_timestamp
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

def _load_catalogue(spark, path: str) -> DataFrame:
    return (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(path)
    )


def build_silver_table(spark, config: dict) -> DataFrame:
    print("===== DÉBUT SILVER =====")
    bronze_db = config["bronze"]["db_name"]
    silver_db = config["silver"]["db_name"]
    catalogue_path = config["silver"]["catalogue_path"]

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")

    bronze_ny = f"{bronze_db}.ventes_bronze_boutique_new_york"
    bronze_paris = f"{bronze_db}.ventes_bronze_boutique_paris"
    bronze_tokyo = f"{bronze_db}.ventes_bronze_boutique_tokyo"

    catalogue_df = _load_catalogue(spark, catalogue_path)

    # NEW YORK
    df_ny_bronze = spark.table(bronze_ny)
    df_ny = (
        df_ny_bronze.alias("b")
        .join(
            catalogue_df.alias("c"),
            (col("b.Product_Name") == col("c.Nom_Produit_Anglais")) &
            (col("b.Category") == col("c.Catégorie_Anglais")),
            "left",
        )
        .select(
            col("b.ID_Sale").cast("int").alias("ID_Vente"),
            col("b.Sale_Date").cast("date").alias("Date_Vente"),
            coalesce(col("c.Nom_Produit_Francais"), col("b.Product_Name")).alias("Nom_Produit"),
            coalesce(col("c.Catégorie_Francais"), col("b.Category")).alias("Catégorie"),
            col("b.Unit_Price").cast("decimal(10,2)").alias("Prix_Unitaire"),
            col("b.Quantity").cast("int").alias("Quantité"),
            (col("b.Unit_Price").cast("decimal(10,2)") * col("b.Quantity").cast("int")).alias("Montant_Total"),
            lit("USD").alias("Devise"),
            lit("Boutique New York").alias("Nom_Boutique"),
            lit("New York").alias("Ville"),
            lit("USA").alias("Pays"),
            col("_ingestion_timestamp"),
            col("_source_file"),
            lit("new_york").alias("_boutique_id"),
        )
    )

    # PARIS
    df_paris_bronze = spark.table(bronze_paris)
    df_paris = (
        df_paris_bronze
        .select(
            col("ID_Vente").cast("int").alias("ID_Vente"),
            col("Date_Vente").cast("date").alias("Date_Vente"),
            col("Nom_Produit").alias("Nom_Produit"),
            col("`Catégorie`").alias("Catégorie"),
            col("Prix_Unitaire").cast("decimal(10,2)").alias("Prix_Unitaire"),
            col("`Quantité`").cast("int").alias("Quantité"),
            (col("Prix_Unitaire").cast("decimal(10,2)") * col("`Quantité`").cast("int")).alias("Montant_Total"),
            lit("EUR").alias("Devise"),
            lit("Boutique Paris").alias("Nom_Boutique"),
            lit("Paris").alias("Ville"),
            lit("France").alias("Pays"),
            col("_ingestion_timestamp"),
            col("_source_file"),
            lit("paris").alias("_boutique_id"),
        )
    )

    # TOKYO
    df_tokyo_bronze = spark.table(bronze_tokyo)
    df_tokyo = (
        df_tokyo_bronze.alias("b")
        .join(
            catalogue_df.alias("c"),
            (col("b.Product_Name") == col("c.Nom_Produit_Anglais")) &
            (col("b.Category") == col("c.Catégorie_Anglais")),
            "left",
        )
        .select(
            col("b.ID_Sale").cast("int").alias("ID_Vente"),
            col("b.Sale_Date").cast("date").alias("Date_Vente"),
            coalesce(col("c.Nom_Produit_Francais"), col("b.Product_Name")).alias("Nom_Produit"),
            coalesce(col("c.Catégorie_Francais"), col("b.Category")).alias("Catégorie"),
            col("b.Unit_Price").cast("decimal(10,2)").alias("Prix_Unitaire"),
            col("b.Quantity").cast("int").alias("Quantité"),
            (col("b.Unit_Price").cast("decimal(10,2)") * col("b.Quantity").cast("int")).alias("Montant_Total"),
            lit("JPY").alias("Devise"),
            lit("Boutique Tokyo").alias("Nom_Boutique"),
            lit("Tokyo").alias("Ville"),
            lit("Japon").alias("Pays"),
            col("_ingestion_timestamp"),
            col("_source_file"),
            lit("tokyo").alias("_boutique_id"),
        )
    )

    df_silver = df_ny.unionByName(df_paris).unionByName(df_tokyo)

    silver_table_name = f"{silver_db}.ventes_unifiees"

    if spark.catalog.tableExists(silver_table_name):
        print(f"Mise à jour incrémentale de {silver_table_name}")
        target_table = DeltaTable.forName(spark, silver_table_name)
        
        target_table.alias("target").merge(
            df_silver.alias("updates"),
            "target.ID_Vente = updates.ID_Vente AND target.Nom_Boutique = updates.Nom_Boutique AND target.Date_Vente = updates.Date_Vente"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        print(f"Création initiale de la table {silver_table_name}")
        df_silver.write.format("delta") \
                 .option("delta.enableChangeDataFeed", "true") \
                 .saveAsTable(silver_table_name)

    print("===== FIN SILVER =====")
    return df_silver
