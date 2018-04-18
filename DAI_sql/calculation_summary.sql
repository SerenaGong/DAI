select 
    account_id, 
    plan_id, 
    last_name, 
    first_name, 
    address_1,
    address_2,
    city,
    state,
    postal_code,
    foundation_id,
    joined_at,
    prev_balance,
    adjustments,
    pmod(account_id,10),
    sum(messages_used) as messages_used,
    sum(mins_used) as mins_used,
    sum(data_used)/1024/1024 as data_used_gb
from
    account_line_agg
group by
    account_id, plan_id , last_name, first_name, address_1,address_2,city,state,postal_code,foundation_id,joined_at,prev_balance,adjustments,pmod(account_id,10)