select
    candid,
    start_date,
    coalesce(sub_days, 90) as sub_days,
    email
from {{ source('raw_data', 'global_table') }}
where start_date is not null
