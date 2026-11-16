# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructors JSON file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC -         Source File
# MAGIC -         Ingestion Timestamp
# MAGIC 3. Write to bronze delta table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

#Define source_file and table_name
source_file=f"{landing_foler_path}/constructors.json"
table_name=f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 1 - Read the JSON file using the dataframe reader API 
# MAGIC

# COMMAND ----------

#Define the schema
constructors_schema="""constructorId STRING, 
                    name STRING,
                     nationality STRING,
                      url STRING""" 

# COMMAND ----------

#Read data from the constructors file
constructors_df= (
    spark.read
    .format("json")
    .schema(constructors_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 2 - Add Metadata Columns
# MAGIC -  Source File
# MAGIC -  Ingestion Timestamp

# COMMAND ----------

constructors_final_df= add_ingestion_metadata(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 3 - Write to bronze delta table
# MAGIC

# COMMAND ----------

(
    constructors_final_df
        .write
        .format("delta")
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

