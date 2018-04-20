import sys
import math
import logging
from itertools import chain
from pyspark.sql.types import * 
from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col, udf

import settings

logging.basicConfig(level=logging.INFO, format=settings.FORMAT)
log = logging.getLogger(__name__)

spark = settings.SPARK

#########
# utility functions
def get_lookup(table, spark_table):
    """Return a Spark DataFrame for a PostgreSQL table.
    """
    retval = None
    log.warning("Retrieving lookup table: {}".format(table))
    try:
        retval = (spark.read.format("jdbc")
                .options(url=settings.PG_URL)
                .options(driver="org.postgresql.Driver")
                .options(user="hwpoc")
                .options(password="hwpoc")
                .options(dbtable=table)
            ).load().cache()

        if spark_table:
            retval.createOrReplaceTempView(spark_table)
            log.warning("Created Spark temp table: {} rows:"
                        " {}".format(spark_table, retval.count()))
    except Exception as e:
        log.execption("Problem retreiving remote lookup table: {}".format(table))
    finally:
        return retval


#######
# spark functions
@udf("integer")
def is_master(phone, line):
    return 1 if phone==line else 0


@udf("float")
def calc_prev_rollover(prev_used, limit):
    retval = 0.0
    try:
        prev_used = float(prev_used)
        limit = float(limit)

        if limit != float('inf'):
            retval = max(0.0, (limit - prev_used))
            if retval != 0.0: 
                log.info("prev_rollover = max(0.0, limit:{} - prev_used:{}) = "
                           "{}".format(limit, prev_used, retval))
    
    except Exception:
        log.exception("Problem calculating previous rollover.")

    finally:
        return retval


@udf("float")
def calc_overage(used, rollover, limit):
    """Calc overage on plan limits.  Applies rollover mins/GB if available.
    """
    retval = 0.0 
    try:
        used = float(used)
        rollover = float(rollover)
        limit = float(limit)
        if float('inf') in [limit, rollover]:
            return 0.0
        else:
            retval = abs(min(0.0, (limit + rollover) - used))
            if retval != 0.0:
                log.info("overage = abs(used:{} - (rollover:{} + limit:{})) = "
                            "{}".format(used, rollover, limit, retval))

    except Exception:
        log.exception("Problem calculating overage amount.")

    finally:
        return retval


# register udfs for access by spark sql
spark.udf.register("is_master", is_master)
spark.udf.register("calc_overage", calc_overage)
spark.udf.register("calc_prev_rollover", calc_prev_rollover)


########
# main functions
def get_raw_data(file_in, schema):
    retval = None   
    try:
        # load datafile
        retval = (spark.read
                  .option("header", "false")
                  .option("mode", "DROPMALFORMED")
                  .schema(schema)
                  .csv(file_in)
                 )
        log.info("Read: {}, Row Count: {}".format(file_in, retval.count()))
    except Exception as e:
        log.execption("Error opening raw data file: {}".format(f))
    finally:
        return retval



# get lookup tables
PLAN = get_lookup("public.plan", "plan")

# build combined list of line-level fees and monthly charges
MONTHLY_CHARGES = get_lookup("public.monthly_charges", "monthly_charges")
OTHER_CHARGES = get_lookup("public.other_charges", "other_charges")
FEES_TAXES = get_lookup("public.fees_taxes", "fees_taxes")
FOUNDATION = get_lookup("public.foundation", "foundation")

raw = get_raw_data(settings.RAW_FILE, settings.RAW_SCHEMA)
raw = raw.withColumn('is_master', is_master(raw["phone"], raw["line"]))
raw.createOrReplaceTempView("raw")
raw.show(5)

# TODO: figure out why removing address_1 for some members produces
# empty dataframes.

# calc line-level detail charges
master_line_sql = """
    with master as (
        select distinct 
            r.account_id, 
            r.line,
            r.is_master,
            r.foundation_id,
            p.plan_name, 
            p.plan_desc, 
            p.base_cost, 
            p.line_cost, 
            p.rollover_voice_min, 
            p.rollover_data_gb, 
            p.adjustment_per_month, 
            p.adjustment_length_month, 
            p.voice_limit_min, 
            p.voice_overage_cost_per_min, 
            p.data_limit_gb, 
            p.data_overage_cost_per_gb, 
            p.text_limit_msg, 
            p.text_overage_cost_per_msg
        from 
            raw r join 
                plan p on (r.plan_id = p.plan_id)
        where r.is_master = 1 
    ) /* select * from master order by rand() limit 10 */
    select * from master
    cluster by account_id
"""
log.info("Creating master_line temp table.")
master_line = spark.sql(master_line_sql)
master_line.show()
master_line.createOrReplaceTempView("master_line")
master_line.cache()
log.warning("Master Line count: {}".format(master_line.count()))

additional_line_sql = """
    with additional as (
        select distinct 
            r.account_id, 
            r.line,
            r.is_master,
            r.foundation_id,
            p.plan_name, 
            p.plan_desc, 
            p.base_cost, 
            p.line_cost, 
            p.rollover_voice_min, 
            p.rollover_data_gb, 
            p.adjustment_per_month, 
            p.adjustment_length_month, 
            p.voice_limit_min, 
            p.voice_overage_cost_per_min, 
            p.data_limit_gb, 
            p.data_overage_cost_per_gb, 
            p.text_limit_msg, 
            p.text_overage_cost_per_msg
        from 
            raw r join 
                plan p on (r.plan_id = p.plan_id)
        where r.is_master = 0 
    ) /* select * from additional order by rand() limit 10 */
    select * from additional 
    cluster by account_id
"""
log.info("Creating additional_line temp table.")
additional_line = spark.sql(additional_line_sql)
additional_line.show()
additional_line.createOrReplaceTempView("additional_line")
additional_line.cache()
log.warning("Additional Line count: {}".format(additional_line.count()))

line_charge_sql = """
    with all_line as (
        select * from master_line union all
        select * from additional_line
    ) /* select * from all_line order by rand() limit 10 */
    , plan_charge as (
        -- only applied to the "master" line
        select
            m.plan_desc name,
            m.base_cost charge,
            m.line,
            m.is_master,
            m.account_id,
            'monthly' grp1,
            cast(NULL as string) grp2
        from
            master_line m
    )
    , master_line_monthly_charge as (
        -- charges only for master lines
        select
            mc.name,
            mc.value charge,
            m.line,
            m.is_master,
            m.account_id,
            'monthly' grp1,
            cast(NULL as string) grp2
        from
            monthly_charges mc,
            master_line m
        where
            mc.line_type in ('both', 'master')
        
    ) /* select * from master_line_monthly_charge order by rand() limit 10 */
    , additional_line_monthly_charge as (
        -- charges only for additional lines
        select
            mc.name,
            mc.value charge,
            a.line,
            a.is_master,
            a.account_id,
            'monthly' grp1,
            cast(NULL as string) grp2
        from
            monthly_charges mc,
            additional_line a 
        where
            mc.line_type in ('both', 'master')
        
    ) /*select * from additional_line_monthly_charge order by rand() limit 10 */
    , foundation_charge as (
        -- master line only
        select
            f.name,
            f.value charge,
            m.line,
            m.is_master,
            m.account_id,
            'monthly' grp1,
            cast(NULL as string) grp2
        from
            foundation f inner join 
                master_line m on (f.foundation_id = m.foundation_id)
    ) /* select * from foundation_charge order by rand() limit 10 */
    , other_charge as (
        select
            oc.name,
            oc.value charge,
            al.line,
            al.is_master,
            al.account_id,
            'other' grp1,
            'fees' grp2
        from
            other_charges oc,
            all_line al
    ) /* select * from other_charge order by rand() limit 10 */
    , fee_tax as (
        select
            ft.name,
            ft.value charge,
            al.line,
            al.is_master,
            al.account_id,
            'other' grp1,
            'taxes' grp2
        from
            fees_taxes ft,
            all_line al
    ) /* select * from other_charge order by rand() limit 10 */
    , all_charge as (
        select * from plan_charge union all
        -- TODO: compute ratio based foundation charges
        select * from foundation_charge union all
        select * from master_line_monthly_charge union all
        select * from additional_line_monthly_charge union all
        select * from other_charge union all
        select * from fee_tax 
    )  /* select * from all_charge order by rand() limit 10 */
    select * 
    from all_charge 
    --where account_id = '1001493346304' 
    cluster by account_id
"""
line_charge = spark.sql(line_charge_sql)
line_charge.createOrReplaceTempView("line_charge")
line_charge.show()

# TODO: Add logic to handle Adjustment logic

log.warning("Computing Voice transaction overages.  Please wait ...")
# voice usage per line
voice_usage_sql = """
    with a as (
        select
            r.account_id,
            r.plan_id,
            r.phone,
            r.line,
            r.prev_voice prev_used,
            p.voice_limit_min lim,
            p.voice_overage_cost_per_min cost,
            sum(mins) curr_used 
        from
            raw r inner join
                plan p on (r.plan_id = p.plan_id)
        where
            txn_type='VOC'
        group by
            1, 2, 3, 4, 5, 6, 7
    ) /* select * from a order by rand() limit 10 */
    , l as (
        select
            a.account_id,
            a.plan_id,
            a.phone, 
            a.line,
            a.prev_used,
            a.lim,
            calc_prev_rollover(a.prev_used, a.lim) rollover,
            a.curr_used,
            a.cost
        from 
            a
    ) /* select * from l order by rand() limit 10 */
    , o as (
        select
            l.account_id,
            l.plan_id,
            l.phone,
            l.line,
            l.curr_used,
            l.rollover,
            l.lim,
            calc_overage(l.curr_used, l.rollover, l.lim) overage,
            l.cost
        from l
    ) /* select * from o order by rand() limit 10 */
    , j as (
        select
            o.account_id,
            o.plan_id,
            o.phone, 
            o.line,
            o.curr_used,
            o.rollover,
            o.lim,
            round(o.overage * o.cost, 2) overage_cost
        from
            o
        where 
            o.overage > 0
    )
    select
        'Voice Overage' name,
        round(sum(j.overage_cost), 2) value,
        j.phone line,
        int(1) is_master,
        j.account_id,
        'monthly' grp1,
        'overage' grp2 
    from
        j
    group by 1, 3, 4, 5, 6, 7
    cluster by j.account_id
    --order by rand()
"""
voice_usage = spark.sql(voice_usage_sql)
voice_usage.createOrReplaceTempView("voice_usage")
voice_usage.show()


# data usage per line
log.warning("Computing Data transaction overages.  Please wait ...")
data_usage_sql = """
    with a as (
        select
            r.account_id,
            r.plan_id,
            r.phone,
            r.line,
            r.prev_data prev_used,
            round(p.data_limit_gb*1024.0*1024.0, 2) lim,
            p.data_overage_cost_per_gb/1024.0/1024.0 cost,
            float(sum(type_unit)) curr_used 
        from
            raw r inner join
                plan p on (r.plan_id = p.plan_id)
        where
            txn_type='DAT'
        group by
            1, 2, 3, 4, 5, 6, 7
    ) /* select * from a order by rand() limit 20 */
    , l as (
        select
            a.account_id,
            a.plan_id,
            a.phone,
            a.line,
            a.prev_used,
            a.lim,
            calc_prev_rollover(a.prev_used, a.lim) rollover,
            a.curr_used,
            a.cost
        from 
            a
    ) /* select * from l order by rand() limit 20 */
    , o as (
        select
            l.account_id,
            l.plan_id,
            l.phone,
            l.line,
            l.curr_used,
            l.rollover,
            l.lim,
            calc_overage(l.curr_used, l.rollover, l.lim) overage,
            l.cost
        from l
    ) /* select * from o order by rand() limit 20 */
    , j as (
        select
            o.account_id,
            o.plan_id,
            o.phone, 
            o.line,
            o.curr_used,
            o.rollover,
            o.lim,
            round(o.overage * o.cost, 2) overage_cost
        from
            o
        where 
            o.overage > 0
    )
    select
        'Data Overage' name,
        round(sum(j.overage_cost), 2) value,
        j.phone line,
        int(1) is_master,
        j.account_id,
        'monthly' grp1,
        'overage' grp2 
    from
        j
    group by 1, 3, 4, 5, 6, 7
    cluster by j.account_id
"""
data_usage = spark.sql(data_usage_sql)
data_usage.createOrReplaceTempView("data_usage")
data_usage.show()

# text usage per line
log.warning("Computing Text Message transaction overages.  Please wait ...")
text_usage_sql = """
    with a as (
        select
            r.account_id,
            r.plan_id,
            r.phone,
            r.line,
            0.0 prev_used,
            p.text_limit_msg lim,
            p.text_overage_cost_per_msg cost,
            count(1) curr_used 
        from
            raw r inner join
                plan p on (r.plan_id = p.plan_id)
        where
            txn_type='TXT'
        group by
            1, 2, 3, 4, 5, 6, 7
    ) /* select * from a order by rand() limit 10 */
    , l as (
        select
            a.account_id,
            a.plan_id,
            a.phone,
            a.line,
            a.prev_used,
            a.lim,
            0 rollover,
            a.curr_used,
            a.cost
        from 
            a
    ) /* select * from l order by rand() limit 10 */
    , o as (
        select
            l.account_id,
            l.plan_id,
            l.phone,
            l.line,
            l.curr_used,
            l.rollover,
            l.lim,
            calc_overage(l.curr_used, l.rollover, l.lim) overage,
            l.cost
        from l
    ) /* select * from o order by rand() limit 10 */
    , j as (
        select
            o.account_id,
            o.plan_id,
            o.phone, 
            o.line,
            o.curr_used,
            o.rollover,
            o.lim,
            round(o.overage * o.cost, 2) overage_cost
        from
            o
        where 
            o.overage > 0
    )
    select
        'Text Messaging Overage' name,
        round(sum(j.overage_cost), 2) value,
        j.phone line,
        int(1) is_master,
        j.account_id,
        'monthly' grp1,
        'overage' grp2 
    from
        j
    group by 1, 3, 4, 5, 6, 7
    cluster by j.account_id
"""
text_usage = spark.sql(text_usage_sql)
text_usage.createOrReplaceTempView("text_usage")
text_usage.show()

# TODO: computer foundation line-level charges for value based calcs
# Add line-level detail record only to master account.

# TODO: computer foundation line-level charges for ratio based calcs
# Add line-level detail record only to master account.
# Compute ratio on a sum of line-level charges that exclude fees-taxes entries.

# line info
log.warning("Aggregatting line-level monthly and overage charges.  Please wait ...")
line_info_sql = """
    with a as (
        select * from line_charge union all
        select * from voice_usage union all
        select * from data_usage union all
        select * from text_usage
    )
    select
        *
    from
        a
    --order by rand()
    cluster by a.account_id
"""
line_info = spark.sql(line_info_sql)
line_info.createOrReplaceTempView("line_info")
line_info.show()


# fake previous months balance relative to current months charges
log.warning("Computing account-level info.  Please wait ...")
account_info_sql = """
    with li as (
        select
            account_id,
            round(sum(charge), 2) charge
        from
            line_info
        group by
            1
    ) /* select * from li order by rand() limit 10 */
    , r as (
        select distinct
            account_id,
            foundation_id,
            last_name,
            first_name,
            phone,
            address_1,
            address_2,
            city,
            state,
            postal_code,
            plan_id,
            prev_balance prev_balance_orig
        from raw
    ) /* select * from r order by rand() limit 20 */
    , a as (
        select
            r.account_id,
            r.prev_balance_orig,
            li.charge,
            (li.charge * 0.10 * (rand() - 0.5)) + li.charge prev_balance_delta
        from
            r join 
                li on (r.account_id = li.account_id)
    ) /* select * from a order by rand() limit 20 */
    , b as (
        select
            a.account_id,
            a.prev_balance_orig,
            a.charge,
            a.prev_balance_delta,
            a.prev_balance_delta + a.charge prev_balance,
            -(a.prev_balance_delta + a.charge - a.prev_balance_orig) prev_payment
        from
            a
    ) /* select * from b order by rand() limit 20 */
    , c as (
        select
            r.account_id,
            r.foundation_id,
            r.last_name,
            r.first_name,
            r.phone,
            r.address_1,
            r.address_2,
            r.city,
            r.state,
            r.postal_code,
            r.plan_id,    
            b.prev_balance,
            b.prev_payment,
            0.0 adjustments,
            b.prev_balance + b.prev_payment + 0.0 balance,
            b.charge,
            b.prev_balance + b.prev_payment + 0.0 + b.charge debit_amount
        from
            b
                inner join r on (b.account_id = r.account_id)
    ) /* select * from c order by rand() limit 10 */
    select 
        * 
    from 
        c
    cluster by 
        account_id

"""
account_info = spark.sql(account_info_sql)
account_info.createOrReplaceTempView("account_info")
account_info.show()

