SELECT 
    trx.account_id,
    trx.plan_id,
    trx.line,
    trx.txn_type,
    trx.last_name,
    trx.first_name,
    trx.address_1,
    trx.address_2,
    trx.city,
    trx.state,
    trx.postal_code,
    trx.foundation_id,
    trx.joined_at,
    trx.prev_balance,
    trx.adjustments,
    txt_plan.messages_used AS messages_used,
    getLine(trx.phone,trx.line) as lineInfo,
    Sum(mins)        AS mins_used,
    Sum(type_unit)   AS data_used
FROM 
    trx_table trx 
        left join plan_table plan on (trx.plan_id = plan.plan_id)
        left join (
            select 
                account_id, 
                line, 
                txn_type, 
                count(sent_recv) as messages_used 
            FROM trx_table 
            where 
                txn_type ='TXT' 
            group by account_id,line,txn_type
        ) txt_plan on (    trx.account_id = txt_plan.account_id 
                       and trx.line = txt_plan.line 
                       and trx.txn_type = txt_plan.txn_type)

group by 
    trx.account_id, 
    trx.plan_id, 
    trx.line, 
    trx.txn_type,
    trx.last_name, 
    trx.first_name, 
    trx.address_1,
    trx.address_2,
    trx.city,
    trx.state,
    trx.postal_code,
    trx.foundation_id,
    trx.joined_at,
    trx.prev_balance,
    trx.adjustments,
    txt_plan.messages_used,
    getLine(trx.phone,trx.line)
order by 
    trx.account_id, 
    trx.plan_id, 
    trx.line, 
    trx.txn_type, 
    trx.last_name, 
    trx.first_name, 
    trx.address_1,
    trx.address_2,
    trx.city,
    trx.state,
    trx.postal_code,
    trx.foundation_id,
    trx.joined_at,
    trx.prev_balance,
    trx.adjustments,
    txt_plan.messages_used,
    getLine(trx.phone, trx.line)


