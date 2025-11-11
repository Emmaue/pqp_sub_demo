-- models/subscriptions/subscription_status.sql

{{ config(materialized='incremental', unique_key='candid') }}

WITH base AS (
    SELECT 
        candid,
        start_date,
        sub_days,
        email
    FROM {{ ref('base_subscription') }}
),

pauses AS (
    SELECT 
        candid,
        total_paused_days
    FROM {{ ref('pause_data') }}
),

extension AS (
    SELECT 
        candid,
        total_extra_days
    FROM {{ ref('extension_data') }}
),

final AS (
    SELECT
        base.candid,
        base.start_date,
        base.sub_days,
        COALESCE(pauses.total_paused_days, 0) AS paused_days,
        COALESCE(extension.total_extra_days, 0) AS extra_days,
        base.email,

        -- adjusted end date calculation
        base.start_date 
        + ((base.sub_days + COALESCE(pauses.total_paused_days, 0) + COALESCE(extension.total_extra_days, 0)) * INTERVAL '1 day')
        AS adjusted_end_date,

        CURRENT_DATE AS today,

        EXTRACT(DAY FROM (
    (
        base.sub_days
        + COALESCE(pauses.total_paused_days, 0)
        + COALESCE(extension.total_extra_days, 0)
    ) * INTERVAL '1 day'
    - (CURRENT_DATE - base.start_date)
)) AS days_left,




        -- expiration flag
        CASE 
            WHEN CURRENT_DATE > (
                base.start_date 
                + ((base.sub_days + COALESCE(pauses.total_paused_days, 0) + COALESCE(extension.total_extra_days, 0)) * INTERVAL '1 day')
            ) THEN TRUE 
            ELSE FALSE 
        END AS is_expired

    FROM base
    LEFT JOIN pauses USING (candid)
    LEFT JOIN extension USING (candid)
)

SELECT * 
FROM final

{% if is_incremental() %}
WHERE candid NOT IN (SELECT candid FROM {{ this }})
{% endif %}
