select
    candid,
    sum(added_days) as total_extra_days
from {{ source('raw_data', 'subscription_extensions') }}
group by candid