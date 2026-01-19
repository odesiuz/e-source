""" Created by: Ono' Uviase
    Date: 2026-01-17
    Time: 7:58p.m.
    Author Email: ono@alexi.com
    File Name: recent_circuit_data.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

spark = SparkSession.builder.getOrCreate()

try:
	from pyspark import pipelines as dp # type: ignore
except ImportError:
	raise ImportError("pyspark.pipelines module is not available in this environment.")

IEDR_CATALOG = spark.conf.get("pipelines.catalog")
NORMALIZED_TABLE = spark.conf.get("input.utilities.table")
TARGET_TABLE= spark.conf.get("target.table")

@dp.table(name=f"`{IEDR_CATALOG}`.gold.{TARGET_TABLE}", table_properties={"quality": "gold"})
def recent_circuit_data():
	return (
		spark.table(f"`{IEDR_CATALOG}`.silver.{NORMALIZED_TABLE}")
		.withColumn("processed_at", current_timestamp())
	)