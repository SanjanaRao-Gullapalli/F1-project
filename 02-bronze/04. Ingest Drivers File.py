# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest drivers.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Define and enforce Schema(preserve the nested structure)
# MAGIC 3. Add Metadata Columns
# MAGIC -      Source File
# MAGIC -      Ingestion Timestamp
# MAGIC 4. Write to bronze detla table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

#Define source_file and table_name
source_file=f"{landing_foler_path}/drivers.json"
table_name=f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 1 - Read the JSON file using the dataframe reader API

# COMMAND ----------

#Define the schema
from pyspark.sql.types import StructType, StructField, StringType,DateType

name_schema= StructType([
    StructField("givenName", StringType()),
    StructField("familyName", StringType())
])

drivers_schema=StructType([
    StructField('driverId',StringType()),
    StructField('name',name_schema),
    StructField('dateOfBirth',DateType()),
    StructField('nationality',StringType()),
    StructField('url',StringType())
])


# COMMAND ----------

#Read data from the constructors file
drivers_df=(
    spark.read
    .format('json')
    .schema(drivers_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 2 - Add Metadata Columns
# MAGIC -      Source File
# MAGIC -      Ingestion Timestamp
# MAGIC

# COMMAND ----------

drivers_final_df= add_ingestion_metadata(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 3 - Write to bronze delta table
# MAGIC

# COMMAND ----------

(
    drivers_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))