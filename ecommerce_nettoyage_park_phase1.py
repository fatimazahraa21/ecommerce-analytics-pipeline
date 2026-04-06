# Databricks notebook source
# Importer la fonction col de PySpark (permet de référencer les colonnes)
from pyspark.sql.functions import col

# ===== ÉTAPE 7: AJOUTER COLONNE CALCULÉE (total_price) =====

# Afficher une ligne de séparation (pour rendre lisible le output)
print("=" * 60)
# Afficher le titre avant l'ajout
print("AVANT AJOUT DE LA COLONNE")
# Afficher une ligne de séparation
print("=" * 60)

# Afficher le texte "Colonnes actuelles:"
print("✅ Colonnes actuelles:")
# Afficher la liste de toutes les colonnes du DataFrame
print(df_clean.columns)

# Afficher le texte "Nombre de lignes:"
print(f"\n✅ Nombre de lignes: {df_clean.count()}")

# ===== MAINTENANT ON AJOUTE LA COLONNE =====

# Utiliser withColumn pour ajouter/remplacer une colonne
df_clean = df_clean.withColumn(
    # Le nom de la nouvelle colonne: 'total_price'
    'total_price',
    # La formule: Quantity (colonne) * UnitPrice (colonne)
    col('Quantity') * col('UnitPrice')
)

# Afficher une ligne vide
print("\n" + "=" * 60)
# Afficher le titre après l'ajout
print("APRÈS AJOUT DE LA COLONNE")
# Afficher une ligne de séparation
print("=" * 60)

# Afficher le texte "Nouvelles colonnes:"
print("✅ Nouvelles colonnes:")
# Afficher la liste de toutes les colonnes (devrait inclure 'total_price' maintenant)
print(df_clean.columns)

# Afficher une ligne vide
print("\n✅ Exemples (Quantity, UnitPrice, total_price):")
# Sélectionner SEULEMENT 3 colonnes: Quantity, UnitPrice, total_price
# show(5, truncate=False) = affiche 5 lignes sans couper le texte
df_clean.select('Quantity', 'UnitPrice', 'total_price').show(5, truncate=False)

# Afficher une ligne de séparation
print("\n" + "=" * 60)
# Afficher le titre final
print("APERÇU COMPLET - 5 PREMIÈRES LIGNES")
# Afficher une ligne de séparation
print("=" * 60)
# Afficher les 5 premières lignes COMPLÈTES (toutes les colonnes)
# truncate=False = ne pas couper les textes longs
df_clean.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col

# ===== ÉTAPE 4: SUPPRIMER LES QUANTITÉS NÉGATIVES OU ZÉRO =====

print("=" * 60)
print("AVANT SUPPRESSION DES prix NÉGATIVES")
print("=" * 60)

# Vérifier combien il y a de prix <= 0
prix_negative = df_clean.filter(col('UnitPrice') <= 0).count()
print(f"❌ Lignes avec prix <= 0: {prix_negative}")

lignes_avant_step4 = df_clean.count()
print(f"✅ Total de lignes: {lignes_avant_step4}")

# Supprimer les lignes avec prix <= 0
df_clean = df_clean.filter(col('UnitPrice') > 0)

print("\n" + "=" * 60)
print("APRÈS SUPPRESSION DES prix NÉGATIVES")
print("=" * 60)

lignes_apres_step4 = df_clean.count()
print(f"✅ Total de lignes: {lignes_apres_step4}")
print(f"❌ Lignes supprimées: {lignes_avant_step4 - lignes_apres_step4}")

# Vérifier qu'il n'y en a plus
prix_negative_after = df_clean.filter(col('UnitPrice') <= 0).count()
print(f"✅ Vérification - prix <= 0 maintenant: {prix_negative_after}")

print("\n" + "=" * 60)
print("APERÇU DES 5 PREMIÈRES LIGNES")
print("=" * 60)
df_clean.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col

# ===== ÉTAPE 4: SUPPRIMER LES QUANTITÉS NÉGATIVES OU ZÉRO =====

print("=" * 60)
print("AVANT SUPPRESSION DES QUANTITÉS NÉGATIVES")
print("=" * 60)

# Vérifier combien il y a de Quantity <= 0
qty_negative = df_clean.filter(col('Quantity') <= 0).count()
print(f"❌ Lignes avec Quantity <= 0: {qty_negative}")

lignes_avant_step4 = df_clean.count()
print(f"✅ Total de lignes: {lignes_avant_step4}")

# Supprimer les lignes avec Quantity <= 0
df_clean = df_clean.filter(col('Quantity') > 0)

print("\n" + "=" * 60)
print("APRÈS SUPPRESSION DES QUANTITÉS NÉGATIVES")
print("=" * 60)

lignes_apres_step4 = df_clean.count()
print(f"✅ Total de lignes: {lignes_apres_step4}")
print(f"❌ Lignes supprimées: {lignes_avant_step4 - lignes_apres_step4}")

# Vérifier qu'il n'y en a plus
qty_negative_after = df_clean.filter(col('Quantity') <= 0).count()
print(f"✅ Vérification - Quantity <= 0 maintenant: {qty_negative_after}")

print("\n" + "=" * 60)
print("APERÇU DES 5 PREMIÈRES LIGNES")
print("=" * 60)
df_clean.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, count, when

# ===== ÉTAPE 3: SUPPRIMER LES LIGNES AVEC NULL =====

print("=" * 60)
print("AVANT SUPPRESSION DES NULL")
print("=" * 60)

lignes_avant = df.count()
print(f"✅ Nombre de lignes: {lignes_avant}")

# Supprimer les lignes avec NULL dans les colonnes importantes
df_clean = df.dropna(subset=['InvoiceNo', 'CustomerID', 'Country', 'Quantity', 'UnitPrice'])

print("\n" + "=" * 60)
print("APRÈS SUPPRESSION DES NULL")
print("=" * 60)

lignes_apres = df_clean.count()
print(f"✅ Nombre de lignes: {lignes_apres}")
print(f"❌ Lignes supprimées: {lignes_avant - lignes_apres}")

# Vérifier qu'il n'y a plus de NULL
print("\n" + "=" * 60)
print("VÉRIFICATION: NULL PAR COLONNE (APRÈS)")
print("=" * 60)

null_counts_after = df_clean.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_clean.columns
]).collect()[0].asDict()

for colonne, null_count in null_counts_after.items():
    if null_count == 0:
        print(f"✅ {colonne}: {null_count} NULL")
    else:
        print(f"❌ {colonne}: {null_count} NULL")

# Afficher quelques lignes pour vérifier
print("\n" + "=" * 60)
print("APERÇU DES 5 PREMIÈRES LIGNES (APRÈS NETTOYAGE)")
print("=" * 60)

df_clean.show(5, truncate=False)

# COMMAND ----------

# On crée une liste de comptage pour voir le valeurs nulls dans chaque colonne
from pyspark.sql.functions import col, count, when
null_counts = df.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df.columns
])
# On affiche le résultat
display(null_counts)


# COMMAND ----------

# Créer une SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when


spark = SparkSession.builder.appName("ecommerce").getOrCreate()

# ✅ CHARGER LES DONNÉES (correctement cette fois!)
df = spark.read.table("ecommerce_data_dirty")

print(f"Données chargées!")
print(f"Lignes: {df.count()}")
print(f"Colonnes: {df.columns}")
display(df)