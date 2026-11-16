-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set-up the project environment for Formula1 Project
-- MAGIC 1. Create External Location databricks-course-ext-dl1428-formula1
-- MAGIC 2. Create Calalog formula1
-- MAGIC
-- MAGIC 3. Create Schemas landing, bronze, silver and gold 
-- MAGIC
-- MAGIC 4. Create Volume files in the landing schema
-- MAGIC

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@dbcourseextdl1428.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create External Location**

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1428_formula1
    URL 'abfss://formula1@dbcourseextdl1428.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `dbcourse-cs`)
    COMMENT 'External location for the formula1 container';


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Create Catalog formula1

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1 
MANAGED LOCATION 'abfss://formula1@dbcourseextdl1428.dfs.core.windows.net/'
COMMENT 'This is the main catalog for the formula1 project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Schemas landing, bronze, silver, gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;
CREATE SCHEMA IF NOT EXISTS formula1.bronze
MANAGED LOCATION 'abfss://formula1@dbcourseextdl1428.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1.silver
MANAGED LOCATION 'abfss://formula1@dbcourseextdl1428.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1.gold
MANAGED LOCATION 'abfss://formula1@dbcourseextdl1428.dfs.core.windows.net/gold';

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

use catalog formula1;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Volume files

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1.landing.files
LOCATION 'abfss://formula1@dbcourseextdl1428.dfs.core.windows.net/landing';


-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files

-- COMMAND ----------

