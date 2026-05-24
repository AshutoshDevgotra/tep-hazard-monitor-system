/*
    Staging model: stg_tep_sensors
    
    Source : raw.tep_faulty (TEP faulty training data)
    Purpose: Rename raw columns to human-readable names, add transformation timestamp,
             and filter out null sample rows.
    
    Materialized as: VIEW
*/

{{ config(materialized='view') }}

SELECT
    "simulationRun"                AS simulation_run,
    "sample"                       AS sample_num,
    "faultNumber"                  AS fault_label,

    -- Process measurement sensors (xmeas)
    "xmeas_1"                      AS feed_a_flow,
    "xmeas_2"                      AS feed_d_flow,
    "xmeas_6"                      AS reactor_feed_rate,
    "xmeas_7"                      AS reactor_pressure,
    "xmeas_8"                      AS reactor_level,
    "xmeas_9"                      AS reactor_temp,
    "xmeas_11"                     AS product_sep_temp,
    "xmeas_13"                     AS product_sep_pressure,
    "xmeas_16"                     AS stripper_pressure,
    "xmeas_18"                     AS stripper_temp,

    -- Manipulated variables (xmv)
    "xmv_1"                        AS d_feed_valve,
    "xmv_3"                        AS recycle_valve,
    "xmv_4"                        AS steam_valve,

    CURRENT_TIMESTAMP              AS transformed_at

FROM {{ source('raw', 'tep_faulty') }}

WHERE "sample" IS NOT NULL
