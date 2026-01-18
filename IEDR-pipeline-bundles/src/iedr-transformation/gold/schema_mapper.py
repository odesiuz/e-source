""" Created by: Ono' Uviase
    Date: 2026-01-18
    Time: 2:12a.m.
    Author Email: ono@alexi.com
    File Name: schema_mapper.py
"""
utility_1_schema_mapping = {
	"ProjectID": "utility_der_id",
	"utility_1": "utility_id",
	"utility_1_ProjectID": "canonical_der_id",
	"NamePlateRating": "capacity_mw",
	"INSTALLED": "status",
	"CASE WHEN SolarPV > 0 → SOLAR, EnergyStorageSystem > 0 → STORAGE": "der_type",
	"ProjectCircuitID": "utility_feeder_id",
	"ingestion timestamp": "last_updated_ts",}

utility_2_schema_mapping = {
	"DER_ID": "utility_der_id",
	"utility_2": "utility_id",
	"utility_2_DER_ID": "canonical_der_id",
	"DER_NAMEPLATE_RATING": "capacity_mw",
	"INSTALLED": "status",
	"DER_TYPE": "der_type",
	"DER_INTERCONNECTION_LOCATION": "utility_feeder_id",
	"ingestion timestamp": "last_updated_ts",}

def get_schema_mapping(utility_id: str) -> dict:
	if utility_id.lower() == "utility_1":
		return utility_1_schema_mapping
	elif utility_id.lower() == "utility_2":
		return utility_2_schema_mapping
	else:
		raise ValueError(f"Unsupported UTILITY_ID: {utility_id}")