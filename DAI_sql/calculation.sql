select 
    account_id, 
    plan_id, 
    line,
    lineInfo, 
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
    sum(messages_used) as messages_used, 
    sum(mins_used) as mins_used, 
    sum(data_used) as data_used 
from 
    account_line_txn_type_agg
where 
        1=1
    and account_id = 1003574397952
group by 
    account_id, plan_id, line, lineInfo ,last_name, first_name, address_1,address_2,city,state,postal_code, foundation_id,joined_at,prev_balance,adjustments
