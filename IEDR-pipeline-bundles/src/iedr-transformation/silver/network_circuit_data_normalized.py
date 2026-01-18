""" Created by: Ono' Uviase
    Date: 2026-01-16
    Time: 8:35p.m.
    Author Email: ono@alexi.com
    File Name: network_circuit_data_normalized.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, max, min

spark = SparkSession.builder.getOrCreate()

try:
	from pyspark import pipelines as dp # type: ignore
except ImportError:
	raise ImportError("pyspark.pipelines module is not available in this environment.")
from schema_mapper import get_schema_mapping

UTILITY_ID = spark.conf.get("utility.id")
IEDR_CATALOG = spark.conf.get("pipelines.catalog")
SCHEMA = spark.conf.get("pipelines.schema")

@dp.table(name=f"`{IEDR_CATALOG}`.{SCHEMA}.{UTILITY_ID}_circuit_normalized_table", table_properties={"quality": "normalized"})
def network_circuit_data_normalized():
	schema_mapping = get_schema_mapping(UTILITY_ID)
	if UTILITY_ID.lower() == "utility_1":
		return (
		spark.read.format("delta")
			.table(f"`{IEDR_CATALOG}`.bronze.{UTILITY_ID}_network_delta_table")
			.selectExpr([f"{old_col} as {new_col}" for old_col, new_col in schema_mapping.items()])
			.withColumn("utility_id", lit(f"{UTILITY_ID}"))
			.groupBy(["utility_id", "utility_feeder_id"])
			.agg(max("feeder_voltage_kv").alias("feeder_voltage_kv"),
	            max("max_hosting_capacity_mw").alias("max_hosting_capacity_mw"),
	            min("min_hosting_capacity_mw").alias("min_hosting_capacity_mw"),
	            max("hosting_capacity_adopted_date").alias("hosting_capacity_adopted_date")
            )
		)
	elif UTILITY_ID.lower() == "utility_2":
		return (
		spark.read.format("delta").table(f"`{IEDR_CATALOG}`.bronze.{UTILITY_ID}_network_delta_table")
			.selectExpr([f"{old_col} as {new_col}" for old_col, new_col in schema_mapping.items()])
			.withColumn("utility_id", lit(f"{UTILITY_ID}"))
		)
	else:
		raise ValueError(f"Unsupported UTILITY_ID: {UTILITY_ID}")
