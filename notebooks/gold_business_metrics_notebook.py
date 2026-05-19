# Databricks notebook source
# =========================================
# GOLD LAYER - BUSINESS METRICS
# =========================================

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# =========================================
# LOAD ENV VARIABLES
# =========================================

load_dotenv()

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

# =========================================
# CREATE SPARK SESSION
# =========================================

spark = SparkSession.builder.getOrCreate()

# =========================================
# CONNECT TO ADLS
# =========================================

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

print("ADLS CONNECTION SUCCESSFUL")

# =========================================
# READ SILVER DATA
# =========================================

silver_path = (
    f"abfss://silver@{storage_account}.dfs.core.windows.net/"
    f"financial_cleaned/"
)

silver_df = spark.read.parquet(silver_path)

display(silver_df)

print(f"Silver row count: {silver_df.count()}")

# =========================================
# CREATE BUSINESS METRICS
# =========================================

gold_df = silver_df \
    .withColumn(
        "profit_margin",
        round(col("net_income") / col("revenue") * 100, 2)
    ) \
    .withColumn(
        "gross_margin",
        round(col("gross_profit") / col("revenue") * 100, 2)
    ) \
    .withColumn(
        "operating_margin",
        round(col("operating_income") / col("revenue") * 100, 2)
    ) \
    .withColumn(
        "profit_ratio",
        round(col("net_income") / col("cost_expenses"), 4)
    )

display(gold_df)

print(f"Gold row count: {gold_df.count()}")

print("GOLD METRICS CREATED SUCCESSFULLY")

# =========================================
# SAVE GOLD DATA TO ADLS
# =========================================

gold_path = (
    f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    f"financial_metrics/"
)

gold_df.coalesce(1).write.mode("overwrite").parquet(gold_path)

print("GOLD PARQUET SAVED SUCCESSFULLY")

# =========================================
# CREATE DELTA DATABASE
# =========================================

spark.sql("CREATE DATABASE IF NOT EXISTS finance_db")

# =========================================
# DROP EXISTING TABLE
# =========================================

spark.sql(
    "DROP TABLE IF EXISTS finance_db.gold_financial_metrics"
)

# =========================================
# SAVE AS DELTA TABLE FOR POWER BI
# =========================================

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("finance_db.gold_financial_metrics")

print("DELTA TABLE SAVED SUCCESSFULLY")

# =========================================
# VERIFY DELTA TABLE
# =========================================

row_count = spark.sql(
    """
    SELECT COUNT(*) AS total
    FROM finance_db.gold_financial_metrics
    """
).collect()[0]["total"]

print(f"Delta table row count: {row_count}")

# =========================================
# DISPLAY FINAL GOLD DATA
# =========================================

display(gold_df)

# =========================================
# VERIFY GOLD LAYER FILES
# =========================================

display(
    dbutils.fs.ls(
        f"abfss://gold@{storage_account}.dfs.core.windows.net/"
    )
)