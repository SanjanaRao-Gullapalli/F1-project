-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Build Constructor Standings 
-- MAGIC
-- MAGIC **Sources**
-- MAGIC
-- MAGIC     1. fact_session_results
-- MAGIC     2. dim_constructors
-- MAGIC
-- MAGIC **Output Columns**
-- MAGIC
-- MAGIC     1. season
-- MAGIC     2. constructor id
-- MAGIC     3. constructor name
-- MAGIC     4. nationality
-- MAGIC     5. race starts
-- MAGIC     6. total points
-- MAGIC     7. number of wins
-- MAGIC     8. number of podiums
-- MAGIC     9. standing position

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.v_constructor_standing
AS 
WITH constructor_session_summary
AS
    (SELECT r.season,
       c.constructor_id,
       c.constructor_name,
       c.nationality,
       COUNT(*) AS race_starts, 
       SUM(points) AS total_points, 
       COUNT_IF(r.is_win) AS number_of_wins,
       COUNT_IF(r.is_podium) AS number_of_podiums
    FROM formula1.gold.fact_session_results r
    JOIN formula1.gold.dim_constructors c
        ON r.constructor_id=c.constructor_id
    GROUP BY r.season,
       c.constructor_id,
       c.constructor_name,
       c.nationality)
select season, 
        constructor_id, 
        constructor_name, 
        nationality, 
        rank() over (partition by season order by total_points DESC,number_of_wins DESC) as standing,
        race_starts, 
        total_points, 
        number_of_wins, 
        number_of_podiums
    from constructor_session_summary;

-- COMMAND ----------

SELECT * FROM formula1.gold.v_constructor_standing WHERE season=2025


-- COMMAND ----------

