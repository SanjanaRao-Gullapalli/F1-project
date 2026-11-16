# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest sprints.json file
# MAGIC 1. Read all the files from the sprints folder using spark dataframe reader API
# MAGIC 2. Define and enforce schema
# MAGIC 3. Add Metadata Columns
# MAGIC -   Source File
# MAGIC -   Ingestion Timestamp
# MAGIC 4. Write to bronze delta table
# MAGIC Note: JSON is in multi line format

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

#Define source_file and table_name
source_file=f"{landing_foler_path}/sprints*.json"
table_name=f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 1 - Read the JSON file using the dataframe reader API 

# COMMAND ----------

#Define the schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

sprints_schema=StructType([
    StructField('date',StringType()), 
    StructField('raceName',StringType()), 
    StructField('round',IntegerType()), 
    StructField('season',IntegerType()), 
    StructField('url',StringType()), 
    StructField('constructorId',StringType()), 
    StructField('driverId',StringType()),
    StructField('grid',IntegerType()), 
    StructField('laps',IntegerType()), 
    StructField('number',IntegerType()), 
    StructField('points',FloatType()), 
    StructField('position',IntegerType()), 
    StructField('positionText',StringType()), 
    StructField('status',StringType()),
])

# COMMAND ----------

#Read data from the sprints file
sprints_df = (

    spark.read
        .format("json")
        .schema(sprints_schema)
        .option('mode','FAILFAST')
        .option('multiLine', True)
        .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 2 - Add Metadata Columns
# MAGIC -    Source File 
# MAGIC -    Ingestion Timestamp

# COMMAND ----------

sprints_final_df= add_ingestion_metadata(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 3 - Write to bronze delta table
# MAGIC

# COMMAND ----------

( 
 sprints_final_df
 .write
 .format('delta')
 .mode('overwrite')
 .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, COUNT(*)
# MAGIC FROM formula1.bronze.sprints
# MAGIC GROUP BY season 
# MAGIC ORDER BY season;

# COMMAND ----------

