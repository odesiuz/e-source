""" Created by: Ono' Uviase
    Date: 2026-01-17
    Time: 5:20p.m.
    Author Email: ono@alexi.com
    File Name: combine_all_normalized_table.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
	from pyspark import pipelines as dp # type: ignore
except ImportError:
	raise ImportError("pyspark.pipelines module is not available in this environment.")

IEDR_CATALOG = spark.conf.get("pipelines.catalog")
SCHEMA = spark.conf.get("pipelines.schema")
TABLE_TO_UNION = spark.conf.get("input.utilities.table")
TARGET_TABLE = spark.conf.get("target.table")

@dp.table(name=f"`{IEDR_CATALOG}`.{SCHEMA}.{TARGET_TABLE}", table_properties={"quality": "silver"})
def all_utilities_network_circuit_data_normalized():
	from functools import reduce

	catalog_db = f"`{IEDR_CATALOG}`.{SCHEMA}"
	tables_df = spark.sql(f"SHOW TABLES IN {catalog_db}")
	table_names = [r.tableName for r in tables_df.collect() if r.tableName.endswith(f"_{TABLE_TO_UNION}")]

	if not table_names:
		raise ValueError("No normalized circuit tables found in silver catalog")

	dfs = [spark.table(f"{catalog_db}.`{name}`") for name in table_names]
	return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)