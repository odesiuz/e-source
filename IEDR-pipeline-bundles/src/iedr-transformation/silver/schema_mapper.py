""" Created by: Ono' Uviase
    Date: 2026-01-16
    Time: 10:02p.m.
    Author Email: ono@alexi.com
    File Name: schema_mapper.py
"""

# dataset keys to schema mapping for different utilities
CIRCUIT = "circuit"
INSTALLED_DER = "installed_der"
PLANNED_DER = "planned_der"

utility_1_circuit_schema_mapping = {
	"NYHCPV_csv_FFEEDER": "utility_feeder_id",
	"NYHCPV_csv_FVOLTAGE": "feeder_voltage_kv",
	"NYHCPV_csv_FMAXHC": "max_hosting_capacity_mw",
	"NYHCPV_csv_FMINHC": "min_hosting_capacity_mw",
	"NYHCPV_csv_FHCADATE": "hosting_capacity_adopted_date"}

utility_2_circuit_schema_mapping = {
	"Master_CDF": "utility_feeder_id",
	"feeder_voltage": "feeder_voltage_kv",
	"feeder_max_hc": "max_hosting_capacity_mw",
	"feeder_min_hc": "min_hosting_capacity_mw",
	"hca_refresh_date": "hosting_capacity_adopted_date"}

utility_1_installed_der_schema_mapping = {
	"ProjectID": "utility_der_id",
	"NamePlateRating": "capacity_mw",
	"der_type": "der_type",
	"ProjectCircuitID": "utility_circuit_id"}

utility_2_installed_der_schema_mapping = {
	"DER_ID": "utility_der_id",
	"DER_NAMEPLATE_RATING": "capacity_mw",
	"DER_TYPE": "der_type",
	"DER_INTERCONNECTION_LOCATION": "utility_circuit_id"}

utility_1_planned_der_schema_mapping = {
	"ProjectID": "utility_der_id",
	"NamePlateRating": "capacity_mw",
	"der_type": "der_type",
	"ProjectCircuitID": "utility_circuit_id"}

utility_2_planned_der_schema_mapping = {
	"INTERCONNECTION_QUEUE_REQUEST_ID": "utility_der_id",
	"DER_NAMEPLATE_RATING": "capacity_mw",
	"DER_TYPE": "der_type",
	"DER_INTERCONNECTION_LOCATION": "utility_circuit_id"}

SCHEMA_REGISTRY = {
	"utility_1": {
		CIRCUIT: utility_1_circuit_schema_mapping,
		INSTALLED_DER: utility_1_installed_der_schema_mapping,
		PLANNED_DER: utility_1_planned_der_schema_mapping,
	},
	"utility_2": {
		CIRCUIT: utility_2_circuit_schema_mapping,
		INSTALLED_DER: utility_2_installed_der_schema_mapping,
		PLANNED_DER: utility_2_planned_der_schema_mapping,
	},
}

def get_schema_mapping(utility_id: str, dataset_key: str) -> dict:
	utility_id_lower = utility_id.lower()
	if utility_id_lower in SCHEMA_REGISTRY:
		if dataset_key in SCHEMA_REGISTRY[utility_id_lower]:
			return SCHEMA_REGISTRY[utility_id_lower][dataset_key]
		else:
			raise ValueError(f"Unsupported dataset key: {dataset_key}")
	else:
		raise ValueError(f"Unsupported UTILITY_ID: {utility_id}")
