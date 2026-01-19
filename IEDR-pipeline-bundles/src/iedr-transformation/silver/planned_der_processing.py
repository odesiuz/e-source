""" Created by: Ono' Uviase
    Date: 2026-01-19
    Time: 4:42a.m.
    Author Email: ono@alexi.com
    File Name: planned_der_processing.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, max, min, concat_ws, col, when, current_timestamp

spark = SparkSession.builder.getOrCreate()

try:
	from pyspark import pipelines as dp  # type: ignore
except ImportError:
	raise ImportError("pyspark.pipelines module is not available in this environment.")
from schema_mapper import get_schema_mapping

UTILITY_ID = spark.conf.get("utility.id")
IEDR_CATALOG = spark.conf.get("pipelines.catalog")
SCHEMA = spark.conf.get("pipelines.schema")


@dp.table(name=f"`{IEDR_CATALOG}`.{SCHEMA}.{UTILITY_ID}_planned_der_normalized_table",
          table_properties={"quality": "normalized"})
def planned_der_data_normalized():
	schema_mapping = get_schema_mapping(utility_id=UTILITY_ID, dataset_key="planned_der")
	if UTILITY_ID.lower() == "utility_1":
		return (
			spark.read.format("delta")
			.table(f"`{IEDR_CATALOG}`.bronze.{UTILITY_ID}_planned_der_delta_table").na.drop(how="all")
			.withColumn("der_type",
			            when(col("SolarPV") > 0, lit("Solar"))
			            .when(col("Wind") > 0, lit("Wind"))
			            .when(col("EnergyStorageSystem") > 0, lit("EnergyStorage"))
			            .when(col("MicroTurbine") > 0, lit("MicroTurbine"))
			            .when(col("FarmWaste") > 0, lit("Bio Gas"))
			            .when(col("FuelCell") > 0, lit("FuelCell"))
			            .when(col("CombinedHeatandPower") > 0, lit("CombinedHeatandPower"))
			            .when(col("GasTurbine") > 0, lit("GasTurbine"))
			            .when(col("Hydro") > 0, lit("Hydro"))
			            .when(col("SteamTurbine") > 0, lit("SteamTurbine"))
			            .otherwise(lit("Other")))
			.selectExpr([f"{old_col} as {new_col}" for old_col, new_col in schema_mapping.items()])
			.withColumn("utility_id", lit(f"{UTILITY_ID}"))
			.withColumn("status", lit("PLANNED"))
			.withColumn("canonical_der_id", concat_ws("_", lit(f"{UTILITY_ID}"), col("utility_der_id")))
			.withColumn("last_updated_ts", current_timestamp())
		)
	elif UTILITY_ID.lower() == "utility_2":
		return (
			spark.read.format("delta")
			.table(f"`{IEDR_CATALOG}`.bronze.{UTILITY_ID}_planned_der_delta_table").na.drop(how="all")
			.withColumn("der_type",
			            when(col("der_type") == "Steam", lit("SteamTurbine"))  # match
			            .when(col("der_type") == "Combined Heat and Power", lit("CombinedHeatandPower"))  # match
			            .when(col("der_type") == "Diesel", lit("MicroTurbine"))
			            .otherwise(col("der_type")))
			.selectExpr([f"{old_col} as {new_col}" for old_col, new_col in schema_mapping.items()])
			.withColumn("utility_id", lit(f"{UTILITY_ID}"))
			.withColumn("status", lit("PLANNED"))
			.withColumn("canonical_der_id", concat_ws("_", lit(f"{UTILITY_ID}"), col("utility_der_id")))
			.withColumn("last_updated_ts", current_timestamp())
		)
	else:
		raise ValueError(f"Unsupported UTILITY_ID: {UTILITY_ID}")
