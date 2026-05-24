/*
    Intermediate model: int_sensor_features

    Source : stg_tep_sensors (staged sensor data)
    Purpose: Compute rolling window statistics (mean, std) for reactor temp
             and pressure over a 10-sample window per simulation_run.
             Flag anomalies using z-score > 3 threshold.

    Materialized as: TABLE
*/

{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_tep_sensors') }}
),

rolling_stats AS (
    SELECT
        simulation_run,
        sample_num,
        fault_label,
        feed_a_flow,
        feed_d_flow,
        reactor_feed_rate,
        reactor_pressure,
        reactor_level,
        reactor_temp,
        product_sep_temp,
        product_sep_pressure,
        stripper_pressure,
        stripper_temp,
        d_feed_valve,
        recycle_valve,
        steam_valve,
        transformed_at,

        -- Rolling 10-sample window statistics per simulation_run
        AVG(reactor_temp) OVER (
            PARTITION BY simulation_run
            ORDER BY sample_num
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS reactor_temp_rolling_mean,

        STDDEV(reactor_temp) OVER (
            PARTITION BY simulation_run
            ORDER BY sample_num
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS reactor_temp_rolling_std,

        AVG(reactor_pressure) OVER (
            PARTITION BY simulation_run
            ORDER BY sample_num
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS reactor_pressure_rolling_mean,

        STDDEV(reactor_pressure) OVER (
            PARTITION BY simulation_run
            ORDER BY sample_num
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS reactor_pressure_rolling_std

    FROM base
),

with_anomalies AS (
    SELECT
        *,

        -- Temperature anomaly: |z-score| > 3
        CASE
            WHEN reactor_temp_rolling_std > 0
                 AND ABS(reactor_temp - reactor_temp_rolling_mean) / reactor_temp_rolling_std > 3
            THEN 1
            ELSE 0
        END AS temp_anomaly_flag,

        -- Pressure anomaly: |z-score| > 3
        CASE
            WHEN reactor_pressure_rolling_std > 0
                 AND ABS(reactor_pressure - reactor_pressure_rolling_mean) / reactor_pressure_rolling_std > 3
            THEN 1
            ELSE 0
        END AS pressure_anomaly_flag

    FROM rolling_stats
)

SELECT
    *,

    -- Combined anomaly: 1 if either temperature or pressure anomaly
    CASE
        WHEN temp_anomaly_flag = 1 OR pressure_anomaly_flag = 1
        THEN 1
        ELSE 0
    END AS combined_anomaly_flag

FROM with_anomalies
