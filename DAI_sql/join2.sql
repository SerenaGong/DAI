SELECT account_id,
       trx.plan_id,
       line,
       txn_type,
       lineInfo,
       sum(sent_recv) AS messages_used,
       Sum(mins)        AS mins_used,
       Sum(type_unit)   AS data_used
       FROM trx_table trx
       left join plan_table plan
       on trx.plan_id = plan.plan_id
        where account_id = 1003574397952
       group by trx.account_id , trx.plan_id, trx.line , trx.txn_type
       order by trx.account_id , trx.plan_id, trx.line , trx.txn_type



