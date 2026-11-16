# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Results Data
# MAGIC 1. Read bronze `results` table
# MAGIC 2. Keep only the columns required for analytics(Drop `url` column)
# MAGIC 3. Standardise column names using snake_case(`constructorId` &rarr; `constructor_id` , `driverId` &rarr; `driver_id` , `raceName` &rarr; `race_name`, `positiveText` &rarr; `finish_position_text`
# MAGIC 4. Rename columns to make them more meaningful (`date` &rarr; `race_date`, `grid` &rarr; `grid_position`, `laps` &rarr; `completed_laps`, `number` &rarr; `car_number`, `position` &rarr; `finish_position`)
# MAGIC 5. Filter out rows where `season`,`round`, `constructor_id` or `driver_id` is null(business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of column `race_name` to Title Case
# MAGIC 8. Write the transformed data to silver `results` table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.results"
silver_table=f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 1 to 4 - Read bronze `results` table, select only the required columns and standardise column names**

# COMMAND ----------

results_df=(
    spark.table(bronze_table)
        .select("season",
               "round",
               "constructorId",
               "driverId",
               "date",
               "raceName",
               "grid",
               "laps",
               "number",
               "points",
               "position",
               "positionText",
               "status",
               "ingestion_timestamp",
               "source_file")
        .withColumnsRenamed({"constructorId":"constructor_id",
                         "driverId":"driver_id",
                         "raceName":"race_name",
                         "date": "race_date",
                         "grid": "grid_position",
                         "laps": "completed_laps",
                         "number": "car_number",
                         "position":"final_position",
                         "positionText":"final_position_text"})
)
        

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 5 & 6 - Apply Data Quality Checks**
# MAGIC - Filter out rows where `season`, `round`, `constructor_id` or `driver_id` is null(business key validation)
# MAGIC - Remove duplicate records
# MAGIC

# COMMAND ----------

results_valid_df=(
    results_df
    .filter(
        F.col("season").isNotNull() & 
        F.col("round").isNotNull() &
        F.col("constructor_id").isNotNull() & 
        F.col("driver_id").isNotNull()
    )
    .drop_duplicates(["season","round","constructor_id","driver_id"])
)

# COMMAND ----------

display(results_df.count()-results_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 7 - Transform values of column `race_name` to Title Case**

# COMMAND ----------

results_final_df=(
    results_valid_df
    .withColumn("race_name",F.initcap(F.col("race_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 8 - Write the transformed data to silver `results` table**

# COMMAND ----------

(
    results_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(silver_table)
)

# COMMAND ----------

print(results_final_df.columns)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

