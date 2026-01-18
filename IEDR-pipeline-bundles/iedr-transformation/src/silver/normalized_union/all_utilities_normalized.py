""" Created by: Ono' Uviase
    Date: 2026-01-17
    Time: 5:20p.m.
    Author Email: ono@alexi.com
    File Name: all_utilities_normalized.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
	from pyspark import pipelines as dp # type: ignore
except ImportError:
	raise ImportError("pyspark.pipelines module is not available in this environment.")

IEDR_CATALOG = "iedr-delta-catalog"

@dp.table(name=f"`{IEDR_CATALOG}`.silver.all_utilities_circuit_normalized_table", table_properties={"quality": "silver"})
def all_utilities_network_circuit_data_normalized():
	from functools import reduce

	catalog_db = f"`{IEDR_CATALOG}`.silver"
	tables_df = spark.sql(f"SHOW TABLES IN {catalog_db}")
	table_names = [r.tableName for r in tables_df.collect() if r.tableName.endswith("_circuit_normalized_table")]

	if not table_names:
		raise ValueError("No normalized circuit tables found in silver catalog")

	dfs = [spark.table(f"{catalog_db}.`{name}`") for name in table_names]
	return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)