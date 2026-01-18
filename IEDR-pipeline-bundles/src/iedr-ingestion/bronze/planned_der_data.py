""" Created by: Ono' Uviase
    Date: 2026-01-14
    Time: 8:19p.m.
    Author Email: ono@alexi.com
    File Name: planned_der_data.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
	from pyspark import pipelines as dp # type: ignore
except ImportError:
	raise ImportError("pyspark.pipelines module is not available in this environment.")

UTILITY_ID = spark.conf.get("utility.id")
IEDR_CATALOG = spark.conf.get("pipelines.catalog")
SCHEMA = spark.conf.get("pipelines.schema")
DATA_SOURCE_PATH = f"/Volumes/workspace/planned_der_data/{UTILITY_ID}/"
BRONZE_TABLE = f"{UTILITY_ID}_planned_der_delta_table"

@dp.table(name=f"`{IEDR_CATALOG}`.{SCHEMA}.{BRONZE_TABLE}", table_properties={"quality": "bronze"})
def planned_der_data_bronze():
	return (spark.read.format("csv").option("header", True)
	        .option("InferSchema", True)
	        .load(f"{DATA_SOURCE_PATH}/*.csv"))
