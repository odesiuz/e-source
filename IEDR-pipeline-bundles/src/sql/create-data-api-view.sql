CREATE OR REPLACE VIEW `workspace`.default.api_der_data_by_circuit AS
    SELECT
        canonical_der_id,
        installed_der_count,
        installed_der_capacity_mw,
        planned_der_count,
        planned_der_capacity_mw
    FROM `iedr-delta-catalog`.gold.recent_installed_and_planed_table;

GRANT SELECT ON workspace.default.api_der_data_by_circuit TO `account users`;

