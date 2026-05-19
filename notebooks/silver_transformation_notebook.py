# Databricks notebook source
# =========================================
# SILVER LAYER - DATA CLEANING
# =========================================

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
# READ RAW DATA
# =========================================

raw_path = (
    f"abfss://raw@{storage_account}.dfs.core.windows.net/"
    f"financial_data/"
)

raw_df = spark.read.json(raw_path)

display(raw_df)

print(f"Raw row count: {raw_df.count()}")

# =========================================
# CLEAN AND SELECT COLUMNS
# =========================================

silver_df = raw_df.select(
    col("acceptedDate").alias("accepted_date"),
    col("symbol").alias("symbol"),
    col("cik").alias("cik"),
    col("revenue").alias("revenue"),
    col("costAndExpenses").alias("cost_expenses"),
    col("netIncome").alias("net_income"),
    col("grossProfit").alias("gross_profit"),
    col("operatingIncome").alias("operating_income"),
    col("ebitda").alias("ebitda"),
    col("eps").alias("eps")
)

display(silver_df)

print(f"Silver row count: {silver_df.count()}")

print("SILVER DATA CREATED SUCCESSFULLY")

# =========================================
# SAVE SILVER DATA
# =========================================

silver_path = (
    f"abfss://silver@{storage_account}.dfs.core.windows.net/"
    f"financial_cleaned/"
)

silver_df.coalesce(1).write.mode("overwrite").parquet(silver_path)

print("SILVER DATA SAVED SUCCESSFULLY")

# =========================================
# VERIFY SILVER LAYER
# =========================================

display(
    dbutils.fs.ls(
        f"abfss://silver@{storage_account}.dfs.core.windows.net/"
    )
)