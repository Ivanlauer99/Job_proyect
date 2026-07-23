# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,🎯 Análisis de Skills Más Demandadas
# MAGIC %md
# MAGIC # 🎯 Análisis de Skills Más Demandadas en Data Engineering
# MAGIC
# MAGIC Este notebook analiza las **habilidades técnicas más solicitadas** en el mercado laboral de Data Engineers mediante el análisis de las descripciones de las ofertas de trabajo.
# MAGIC
# MAGIC ## Metodología
# MAGIC
# MAGIC Extraemos skills mencionadas en las descripciones (`job_description` y `job_title`) buscando palabras clave de tecnologías, herramientas y frameworks comunes en Data Engineering:
# MAGIC
# MAGIC ### Categorías de Skills:
# MAGIC * **Lenguajes**: Python, SQL, Scala, Java, R
# MAGIC * **Big Data**: Spark, Hadoop, Kafka, Flink
# MAGIC * **Cloud**: AWS, Azure, GCP, Databricks, Snowflake
# MAGIC * **ETL/Orchestration**: Airflow, dbt, Luigi, Dagster
# MAGIC * **Databases**: PostgreSQL, MySQL, MongoDB, Cassandra, Redis
# MAGIC * **Data Warehouses**: Redshift, BigQuery, Synapse
# MAGIC * **Containerización**: Docker, Kubernetes
# MAGIC * **CI/CD**: Jenkins, GitLab CI, GitHub Actions

# COMMAND ----------

# DBTITLE 1,🛠️ Vista: Skills Extraidas de Ofertas
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- 🛠️ VISTA: SKILLS EXTRAÍDAS DE OFERTAS
# MAGIC -- ============================================
# MAGIC -- Extrae skills mencionadas en títulos y descripciones de ofertas
# MAGIC -- Cada fila representa una oferta con flags indicando qué skills menciona
# MAGIC
# MAGIC CREATE OR REPLACE VIEW prueba_api.semantic.vw_job_skills
# MAGIC COMMENT 'Skills técnicas extraídas de descripciones y títulos de ofertas'
# MAGIC AS
# MAGIC WITH job_text AS (
# MAGIC   SELECT 
# MAGIC     job_id,
# MAGIC     employer_sk,
# MAGIC     location_sk,
# MAGIC     employment_type_sk,
# MAGIC     job_title,
# MAGIC     job_is_remote,
# MAGIC     job_salary_avg,
# MAGIC     -- Combinar título y descripción en un solo campo para búsqueda
# MAGIC     UPPER(CONCAT(COALESCE(job_title, ''), ' ', COALESCE(job_description, ''))) AS full_text
# MAGIC   FROM prueba_api.gold.fact_jobs
# MAGIC )
# MAGIC SELECT 
# MAGIC   job_id,
# MAGIC   employer_sk,
# MAGIC   location_sk,
# MAGIC   employment_type_sk,
# MAGIC   job_title,
# MAGIC   job_is_remote,
# MAGIC   job_salary_avg,
# MAGIC   
# MAGIC   -- Lenguajes de Programación
# MAGIC   CASE WHEN full_text LIKE '%PYTHON%' OR full_text LIKE '%PYSPARK%' THEN 1 ELSE 0 END AS has_python,
# MAGIC   CASE WHEN full_text LIKE '%SQL%' OR full_text LIKE '%T-SQL%' OR full_text LIKE '%TSQL%' THEN 1 ELSE 0 END AS has_sql,
# MAGIC   CASE WHEN full_text LIKE '%SCALA%' THEN 1 ELSE 0 END AS has_scala,
# MAGIC   CASE WHEN full_text LIKE '%JAVA%' THEN 1 ELSE 0 END AS has_java,
# MAGIC   CASE WHEN full_text LIKE '% R %' OR full_text LIKE '%R PROGRAMMING%' THEN 1 ELSE 0 END AS has_r,
# MAGIC   
# MAGIC   -- Big Data & Procesamiento
# MAGIC   CASE WHEN full_text LIKE '%SPARK%' OR full_text LIKE '%APACHE SPARK%' THEN 1 ELSE 0 END AS has_spark,
# MAGIC   CASE WHEN full_text LIKE '%HADOOP%' THEN 1 ELSE 0 END AS has_hadoop,
# MAGIC   CASE WHEN full_text LIKE '%KAFKA%' THEN 1 ELSE 0 END AS has_kafka,
# MAGIC   CASE WHEN full_text LIKE '%FLINK%' THEN 1 ELSE 0 END AS has_flink,
# MAGIC   
# MAGIC   -- Cloud Platforms
# MAGIC   CASE WHEN full_text LIKE '%AWS%' OR full_text LIKE '%AMAZON WEB SERVICES%' THEN 1 ELSE 0 END AS has_aws,
# MAGIC   CASE WHEN full_text LIKE '%AZURE%' OR full_text LIKE '%MICROSOFT AZURE%' THEN 1 ELSE 0 END AS has_azure,
# MAGIC   CASE WHEN full_text LIKE '%GCP%' OR full_text LIKE '%GOOGLE CLOUD%' THEN 1 ELSE 0 END AS has_gcp,
# MAGIC   CASE WHEN full_text LIKE '%DATABRICKS%' THEN 1 ELSE 0 END AS has_databricks,
# MAGIC   CASE WHEN full_text LIKE '%SNOWFLAKE%' THEN 1 ELSE 0 END AS has_snowflake,
# MAGIC   
# MAGIC   -- ETL & Orquestación
# MAGIC   CASE WHEN full_text LIKE '%AIRFLOW%' THEN 1 ELSE 0 END AS has_airflow,
# MAGIC   CASE WHEN full_text LIKE '%DBT%' OR full_text LIKE '%DATA BUILD TOOL%' THEN 1 ELSE 0 END AS has_dbt,
# MAGIC   CASE WHEN full_text LIKE '%ETL%' THEN 1 ELSE 0 END AS has_etl,
# MAGIC   CASE WHEN full_text LIKE '%LUIGI%' THEN 1 ELSE 0 END AS has_luigi,
# MAGIC   
# MAGIC   -- Data Warehouses
# MAGIC   CASE WHEN full_text LIKE '%REDSHIFT%' THEN 1 ELSE 0 END AS has_redshift,
# MAGIC   CASE WHEN full_text LIKE '%BIGQUERY%' OR full_text LIKE '%BIG QUERY%' THEN 1 ELSE 0 END AS has_bigquery,
# MAGIC   CASE WHEN full_text LIKE '%SYNAPSE%' THEN 1 ELSE 0 END AS has_synapse,
# MAGIC   
# MAGIC   -- Databases
# MAGIC   CASE WHEN full_text LIKE '%POSTGRES%' THEN 1 ELSE 0 END AS has_postgresql,
# MAGIC   CASE WHEN full_text LIKE '%MYSQL%' THEN 1 ELSE 0 END AS has_mysql,
# MAGIC   CASE WHEN full_text LIKE '%MONGODB%' OR full_text LIKE '%MONGO DB%' THEN 1 ELSE 0 END AS has_mongodb,
# MAGIC   CASE WHEN full_text LIKE '%CASSANDRA%' THEN 1 ELSE 0 END AS has_cassandra,
# MAGIC   CASE WHEN full_text LIKE '%REDIS%' THEN 1 ELSE 0 END AS has_redis,
# MAGIC   
# MAGIC   -- Containerización & Orquestación
# MAGIC   CASE WHEN full_text LIKE '%DOCKER%' THEN 1 ELSE 0 END AS has_docker,
# MAGIC   CASE WHEN full_text LIKE '%KUBERNETES%' OR full_text LIKE '%K8S%' THEN 1 ELSE 0 END AS has_kubernetes,
# MAGIC   
# MAGIC   -- CI/CD
# MAGIC   CASE WHEN full_text LIKE '%JENKINS%' THEN 1 ELSE 0 END AS has_jenkins,
# MAGIC   CASE WHEN full_text LIKE '%GITLAB%' THEN 1 ELSE 0 END AS has_gitlab,
# MAGIC   CASE WHEN full_text LIKE '%GITHUB ACTIONS%' THEN 1 ELSE 0 END AS has_github_actions,
# MAGIC   
# MAGIC   -- Servicios AWS específicos
# MAGIC   CASE WHEN full_text LIKE '%S3%' OR full_text LIKE '%SIMPLE STORAGE SERVICE%' THEN 1 ELSE 0 END AS has_s3,
# MAGIC   CASE WHEN full_text LIKE '%EMR%' OR full_text LIKE '%ELASTIC MAPREDUCE%' THEN 1 ELSE 0 END AS has_emr,
# MAGIC   CASE WHEN full_text LIKE '%LAMBDA%' OR full_text LIKE '%AWS LAMBDA%' THEN 1 ELSE 0 END AS has_lambda,
# MAGIC   CASE WHEN full_text LIKE '%GLUE%' OR full_text LIKE '%AWS GLUE%' THEN 1 ELSE 0 END AS has_glue,
# MAGIC   
# MAGIC   -- Machine Learning & AI
# MAGIC   CASE WHEN full_text LIKE '%MACHINE LEARNING%' OR full_text LIKE '%ML%' THEN 1 ELSE 0 END AS has_ml,
# MAGIC   CASE WHEN full_text LIKE '%TENSORFLOW%' THEN 1 ELSE 0 END AS has_tensorflow,
# MAGIC   CASE WHEN full_text LIKE '%PYTORCH%' THEN 1 ELSE 0 END AS has_pytorch,
# MAGIC   CASE WHEN full_text LIKE '%SCIKIT%' OR full_text LIKE '%SKLEARN%' THEN 1 ELSE 0 END AS has_scikit
# MAGIC   
# MAGIC FROM job_text;

# COMMAND ----------

# DBTITLE 1,📊 Ejemplos de Análisis
# MAGIC %md
# MAGIC ## 📊 Ejemplos de Análisis de Skills
# MAGIC
# MAGIC A continuación encontrarás diferentes análisis sobre las skills más demandadas en el mercado.

# COMMAND ----------

# DBTITLE 1,Ejemplo 4: Skills Más Demandadas por País
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- EJEMPLO 4: SKILLS MÁS DEMANDADAS POR PAÍS
# MAGIC -- ============================================
# MAGIC -- Analiza diferencias regionales en la demanda de skills
# MAGIC
# MAGIC SELECT 
# MAGIC   l.job_country AS pais,
# MAGIC   SUM(s.has_python) AS python,
# MAGIC   SUM(s.has_sql) AS sql,
# MAGIC   SUM(s.has_spark) AS spark,
# MAGIC   SUM(s.has_aws) AS aws,
# MAGIC   SUM(s.has_azure) AS azure,
# MAGIC   SUM(s.has_gcp) AS gcp,
# MAGIC   SUM(s.has_databricks) AS databricks,
# MAGIC   SUM(s.has_docker) AS docker,
# MAGIC   SUM(s.has_kubernetes) AS kubernetes,
# MAGIC   COUNT(*) AS total_ofertas
# MAGIC FROM prueba_api.gold.vw_job_skills s
# MAGIC INNER JOIN prueba_api.gold.vw_market_overview l 
# MAGIC   ON s.job_id = l.job_id
# MAGIC WHERE l.job_country IS NOT NULL
# MAGIC GROUP BY l.job_country
# MAGIC ORDER BY total_ofertas DESC;

# COMMAND ----------


