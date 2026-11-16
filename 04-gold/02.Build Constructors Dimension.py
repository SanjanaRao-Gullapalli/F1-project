# Databricks notebook source
# MAGIC %md
# MAGIC #Build Constructors Dimension
# MAGIC 1. Read silver `constructors` table
# MAGIC 2. Read gold `ref_nationality_region` table
# MAGIC 3. Join the data from `constructors` with `ref_nationality_region` using `nationality` 
# MAGIC 4. Select the required columns
# MAGIC         -   constructors.constructor_id
# MAGIC         -   constructors.constructor_name
# MAGIC         -   constructors.nationality
# MAGIC         -   ref_nationality_region.region
# MAGIC 5. Write the transformed data to gold `dim_constructors` table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config 

# COMMAND ----------

target_table=f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 1 - Read source tables**
# MAGIC  -  `silver.constructors`
# MAGIC  -  `gold.ref_nationality_region`

# COMMAND ----------

constructors_df=spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df=spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 2 - Join `constructors` with `nationality_region_df` using `nationality`**
# MAGIC
# MAGIC Select the following columns
# MAGIC  1. constructors.constructor_id
# MAGIC  2. constructors.constructor_name
# MAGIC  3. constructors.nationality
# MAGIC  4. ref_nationality_region.region

# COMMAND ----------

dim_constructors_df=(
    constructors_df
        .join(
            ref_nationality_region_df,
            constructors_df.nationality==ref_nationality_region_df.nationality,
            "left_outer")
        .select(
            constructors_df.constructor_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **STEP 3 - Write the transformed data to the `gold` `dim_constructors` table**

# COMMAND ----------

(
    dim_constructors_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

