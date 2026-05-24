/*
    Mart model: fault_summary

    Source : int_sensor_features (engineered features)
    Purpose: Aggregate per-fault-type statistics for dashboard and reporting.
             Includes sample counts, mean sensor readings, and anomaly rates.

    Materialized as: TABLE
*/

{{ config(materialized='table') }}

SELECT
    fault_label,
    COUNT(*)                                                 AS total_samples,
    ROUND(AVG(reactor_temp)::NUMERIC, 2)                     AS avg_reactor_temp,
    ROUND(AVG(reactor_pressure)::NUMERIC, 2)                 AS avg_reactor_pressure,
    SUM(combined_anomaly_flag)                                AS anomaly_count,
    ROUND(
        (SUM(combined_anomaly_flag)::NUMERIC / COUNT(*)) * 100,
        2
    )                                                        AS anomaly_pct

FROM {{ ref('int_sensor_features') }}

GROUP BY fault_label
ORDER BY fault_label
