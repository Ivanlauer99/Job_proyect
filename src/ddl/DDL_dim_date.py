# Databricks notebook source
# MAGIC %sql
# MAGIC -- Crear dimensión de fechas con clave surrogada autoincremental
# MAGIC CREATE OR REPLACE TABLE prueba_api.gold.dim_date (
# MAGIC     date_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     fecha DATE,
# MAGIC     año INT,
# MAGIC     mes INT,
# MAGIC     dia INT,
# MAGIC     nombre_mes STRING,
# MAGIC     nombre_mes_corto STRING,
# MAGIC     trimestre INT,
# MAGIC     semestre INT,
# MAGIC     dia_semana INT,
# MAGIC     nombre_dia_semana STRING,
# MAGIC     nombre_dia_semana_corto STRING,
# MAGIC     semana_año INT,
# MAGIC     dia_año INT,
# MAGIC     es_fin_semana BOOLEAN,
# MAGIC     es_primer_dia_mes BOOLEAN,
# MAGIC     es_ultimo_dia_mes BOOLEAN,
# MAGIC     fecha_carga TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Dimensión de fechas con clave surrogada autoincremental (IDENTITY)';

# COMMAND ----------


