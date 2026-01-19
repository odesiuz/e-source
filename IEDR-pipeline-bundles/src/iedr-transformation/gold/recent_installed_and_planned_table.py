""" Created by: Ono' Uviase
    Date: 2026-01-19
    Time: 7:01a.m.
    Author Email: ono@alexi.com
    File Name: recent_installed_and_planned_table.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum
from functools import reduce

try:
	from pyspark import pipelines as dp # type: ignore
except ImportError:
	raise ImportError("pyspark.pipelines module is not available in this environment.")


spark = SparkSession.builder.getOrCreate()

IEDR_CATALOG = spark.conf.get("pipelines.catalog")
SCHEMA = spark.conf.get("pipelines.schema")
TARGET_TABLE= spark.conf.get("target.table")

@dp.table(name=f"`{IEDR_CATALOG}`.{SCHEMA}.{TARGET_TABLE}", table_properties={"quality": "gold"})
def combine_install_and_planned_table():
	rows = spark.sql(f"SHOW TABLES IN `{IEDR_CATALOG}`.silver").collect()
	names = [r.tableName for r in rows if r.tableName.startswith("combined_") and r.tableName.endswith("_der_table")]
	if not names:
		raise RuntimeError("no matching tables found")
	dfs = [spark.table(f"`{IEDR_CATALOG}`.silver.{n}") for n in names]
	unioned = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)  # allowMissingColumns if schemas vary
	result_df = unioned.groupBy("canonical_der_id").agg(
	    sum(when(col("status") == "INSTALLED", 1)).alias("installed_der_count"),
	    sum(when(col("status") == "INSTALLED", col("capacity_mw"))).alias("installed_der_capacity_mw"),
	    sum(when(col("status") == "PLANNED", 1)).alias("planned_der_count"),
	    sum(when(col("status") == "PLANNED", col("capacity_mw"))).alias("planned_der_capacity_mw")
	)
	return result_df