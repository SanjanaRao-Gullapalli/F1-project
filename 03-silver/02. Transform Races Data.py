# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Races Data
# MAGIC 1. Read bronze `races` table
# MAGIC 2. Keep only the columns required for analytics(Drop `url` column)
# MAGIC 3. Standardise column names using snake_case(`raceName` &rarr; `race_name` , `circuitId` &rarr; `circuit_id`)
# MAGIC 4. Rename column to make them more meaningful(`date` &rarr; `race_date`)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of column `race_name` to Title Case
# MAGIC 7. Write the transformed data to silver `races` table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.races"
silver_table=f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md 
# MAGIC **STEP 1 - Read bronze `races` table**

# COMMAND ----------

races_df=spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 2 - Keep only the columns required for analytics(Drop url column)**

# COMMAND ----------

races_selected_df=races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 & 4 - Standaradise Column Names**
# MAGIC - Standardise column names using snake_case ( `circuitId` &rarr; `circuit_id` , `raceName` &rarr; `race_name`)
# MAGIC - Rename columns to make them more meaningful(`date` &rarr; `race_date`)

# COMMAND ----------

races_renamed_df=(
    races_selected_df
    .withColumnsRenamed({"circuitId":"circuit_id","date":"race_date","raceName":"race_name", "date": "race_date"})
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 - Remove duplicate records**

# COMMAND ----------

races_distinct_df=races_renamed_df.dropDuplicates(['season','round'])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC **STEP 6 - Transform values of column `race_name` to Title Case**

# COMMAND ----------

races_final_df=(races_distinct_df
               .withColumn('race_name',F.initcap(F.col("race_name"))))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 - Write the transformed data to silver `races` table**

# COMMAND ----------

( 
 races_final_df
 .write
 .format('delta')
 .mode('overwrite')
 .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

