SELECT 
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
    msg_cost,
    voice_cost,
    data_cost,
    (msg_cost+voice_cost+data_cost) AS total 
from (
    SELECT     
        a.account_id,
        a.plan_id,
        a.last_name,
        a.first_name,
        a.address_1,
        a.address_2,
        a.city,
        a.state,
        a.postal_code,
        a.foundation_id,
        a.joined_at,
        a.prev_balance,
        a.adjustments,
        calculateMessageFee(a.messages_used,
                            p.text_limit_msg,
                            p.text_overage_cost_per_msg) as msg_cost,
        CASE WHEN 
            a.mins_used > p.voice_limit_min then 
                (a.mins_used - p.voice_limit_min) * p.voice_overage_cost_per_min 
            else 
                0 
        end as voice_cost,
        CASE WHEN 
            a.data_used_gb > p.data_limit_gb then 
                (a.data_used_gb - p.data_limit_gb) * p.data_overage_cost_per_gb 
            else 
                0 
        end as data_cost
    from 
        account_usage a LEFT JOIN plan_table p on (a.plan_id = p.plan_id)

) a
