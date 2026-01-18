""" Created by: Ono' Uviase
    Date: 2026-01-16
    Time: 10:02p.m.
    Author Email: ono@alexi.com
    File Name: schema_mapper.py
"""
utility_1_schema_mapping = {
	"NYHCPV_csv_FFEEDER": "utility_feeder_id",
	"NYHCPV_csv_FVOLTAGE": "feeder_voltage_kv",
	"NYHCPV_csv_FMAXHC": "max_hosting_capacity_mw",
	"NYHCPV_csv_FMINHC": "min_hosting_capacity_mw",
	"NYHCPV_csv_FHCADATE": "hosting_capacity_adopted_date"}

utility_2_schema_mapping = {
	"Master_CDF": "utility_feeder_id",
	"feeder_voltage": "feeder_voltage_kv",
	"feeder_max_hc": "max_hosting_capacity_mw",
	"feeder_min_hc": "min_hosting_capacity_mw",
	"hca_refresh_date": "hosting_capacity_adopted_date"}

def get_schema_mapping(utility_id: str) -> dict:
	if utility_id == "utility_1":
		return utility_1_schema_mapping
	elif utility_id == "utility_2":
		return utility_2_schema_mapping
	else:
		raise ValueError(f"Unsupported UTILITY_ID: {utility_id}")