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

IEDR_CATALOG = "iedr-delta-catalog"
TABLE_NAME = "recent_circuit_data_table"

@dp.table(name=f"`{IEDR_CATALOG}`.gold.{TABLE_NAME}", table_properties={"quality": "gold"})
def recent_circuit_data():
	return (
		spark.table(f"`{IEDR_CATALOG}`.silver.all_utilities_circuit_normalized_table")
		.withColumn("processed_at", current_timestamp())
	)