# Sales Pipeline – Databricks (TP Industrialisation Spark)

## 🎯 Objectif du projet

Ce projet a pour objectif d’industrialiser un traitement Spark développé initialement sous forme de notebooks Databricks.

Il implémente un pipeline de traitement des données de ventes d’une entreprise de retail, en suivant l’architecture **Médaillon** :

- **Bronze** : ingestion des données brutes des boutiques
- **Silver** : nettoyage, normalisation et enrichissement des données
- **Gold** : calcul des indicateurs métiers (chiffre d’affaires, classements produits)

Le projet est structuré comme une application Python packagée, exécutable et maintenable.

---

## 🧱 Architecture du pipeline


- **Bronze** : lecture des fichiers CSV mensuels par boutique et stockage en tables Delta
- **Silver** : harmonisation des schémas, traduction des libellés, enrichissement géographique
- **Gold** : agrégations et calcul des KPI en EUR

---

## ▶️ Exécution du pipeline

Le pipeline est exécuté via le script `main.py`.

Un fichier de configuration YAML est utilisé pour définir :
- les chemins de données,
- les noms de bases,
- les taux de conversion.

### Exemple d’exécution depuis Databricks (notebook)

```python
from main import main

main(dbutils=dbutils)
