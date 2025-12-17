from pyspark.sql.functions import col, sum as _sum, date_format, desc, current_timestamp
from pyspark.sql import DataFrame

def build_gold_tables(spark, config: dict) -> None:
    print("===== DÉBUT GOLD =====")
    silver_db = config["silver"]["db_name"]
    gold_cfg = config["gold"]
    gold_db = gold_cfg["db_name"]

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")

    silver_table = f"{silver_db}.ventes_unifiees"
    df_silver = spark.table(silver_table)

    # Taux de change
    rates = gold_cfg["rates"]
    data_rates = [(dev, float(rate)) for dev, rate in rates.items()]
    taux_change_df = spark.createDataFrame(data_rates, ["Devise", "Taux_EUR"])

    df_fact = (
        df_silver.alias("s")
        .join(taux_change_df.alias("t"), on="Devise", how="left")
        .withColumn("Montant_EUR", (col("Montant_Total") * col("Taux_EUR")).cast("decimal(18,2)"))
        .withColumn("Annee_Mois", date_format(col("Date_Vente"), "yyyy-MM"))
        .withColumn("_gold_ingestion_timestamp", current_timestamp())
    )

    fact_table = f"{gold_db}.ventes_gold_fact"
    df_fact.write.format("delta").mode("overwrite").saveAsTable(fact_table)

    # CA global mensuel
    df_ca_global_mensuel = (
        df_fact.groupBy("Annee_Mois")
               .agg(_sum("Montant_EUR").alias("CA_EUR"))
               .orderBy("Annee_Mois")
    )
    df_ca_global_mensuel.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{gold_db}.kpi_ca_global_mensuel")

    # CA par boutique
    df_ca_boutique_mensuel = (
        df_fact.groupBy("Annee_Mois", "Nom_Boutique")
               .agg(_sum("Montant_EUR").alias("CA_EUR"))
               .orderBy("Annee_Mois", "Nom_Boutique")
    )
    df_ca_boutique_mensuel.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{gold_db}.kpi_ca_boutique_mensuel")

    # Top produits par quantité
    df_top_qte = (
        df_fact.groupBy("Nom_Produit")
               .agg(_sum("Quantité").alias("Quantite_Totale"))
               .orderBy(desc("Quantite_Totale"))
    )
    df_top_qte.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{gold_db}.kpi_top_produits_quantite")

    # Top produits par CA
    df_top_ca = (
        df_fact.groupBy("Nom_Produit")
               .agg(_sum("Montant_EUR").alias("CA_Total_EUR"))
               .orderBy(desc("CA_Total_EUR"))
    )
    df_top_ca.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{gold_db}.kpi_top_produits_ca")

    print("===== FIN GOLD =====")
