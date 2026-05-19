# Databricks notebook source
# =========================================
# RAW LAYER - API INGESTION
# =========================================

import os
import requests
import pandas as pd

from pyspark.sql import SparkSession
from dotenv import load_dotenv

# =========================================
# LOAD ENV VARIABLES
# =========================================

load_dotenv()

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")
api_key = os.getenv("FMP_API_KEY")

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
# COMPANY SYMBOLS
# =========================================

symbols = ["AAPL", "MSFT", "TSLA", "AMZN", "GOOGL"]

# =========================================
# FETCH API DATA
# =========================================

all_data = []

for symbol in symbols:

    url = (
        f"https://financialmodelingprep.com/stable/income-statement"
        f"?symbol={symbol}&apikey={api_key}"
    )

    response = requests.get(url)

    if response.status_code == 200:

        data = response.json()

        if isinstance(data, list) and len(data) > 0:

            all_data.extend(data)

            print(f"✓ {symbol}: {len(data)} records fetched")

        else:
            print(f"✗ {symbol}: No data returned")

    else:
        print(f"✗ {symbol}: API error {response.status_code}")

print(f"\nTotal records fetched: {len(all_data)}")

# =========================================
# CONVERT TO PANDAS DATAFRAME
# =========================================

df = pd.DataFrame(all_data)

display(df)

print(f"Pandas shape: {df.shape}")

# =========================================
# CONVERT TO SPARK DATAFRAME
# =========================================

spark_df = spark.createDataFrame(df)

display(spark_df)

print(f"Spark row count: {spark_df.count()}")

# =========================================
# SAVE INTO RAW LAYER
# =========================================

raw_path = (
    f"abfss://raw@{storage_account}.dfs.core.windows.net/"
    f"financial_data/"
)

spark_df.coalesce(1).write.mode("overwrite").json(raw_path)

print("RAW DATA SAVED SUCCESSFULLY")

# =========================================
# VERIFY FILES IN RAW LAYER
# =========================================

display(
    dbutils.fs.ls(
        f"abfss://raw@{storage_account}.dfs.core.windows.net/"
    )
)