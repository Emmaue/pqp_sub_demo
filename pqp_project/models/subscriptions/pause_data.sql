select
    candid,
    sum(resume_date - pause_date) as total_paused_days
from {{ source('raw_data', 'subscription_pauses') }}
group by candid