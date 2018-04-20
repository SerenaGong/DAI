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
def value_or_ratio(value, ratio):
    return value if value is not None else ratio


@udf("integer")
def is_master(phone, line):
    return 1 if phone==line else 0


def calculate_message_fee(messages_used, text_limit_msg, text_overage_cost_per_msg):
    if messages_used is None or text_limit_msg is None or messages_used <= text_limit_msg:
        return 0
    else:
        return (messages_used - text_limit_msg) * text_overage_cost_per_msg


def get_base(line, base_cost, line_cost):
    return base_cost if line == "master" else line_cost


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

# spark.udf.register("valueRatio", value_or_ratio)
# spark.udf.register("getBase", get_base)
# spark.udf.register("calculateMessageFee", calculate_message_fee)


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

# # TODO: figure out why removing address_1 for some members produces
# # empty dataframes.
# 
# # calc line-level detail charges
# master_line_sql = """
#     with master as (
#         select distinct 
#             r.account_id, 
#             r.line,
#             r.is_master,
#             r.foundation_id,
#             p.plan_name, 
#             p.plan_desc, 
#             p.base_cost, 
#             p.line_cost, 
#             p.rollover_voice_min, 
#             p.rollover_data_gb, 
#             p.adjustment_per_month, 
#             p.adjustment_length_month, 
#             p.voice_limit_min, 
#             p.voice_overage_cost_per_min, 
#             p.data_limit_gb, 
#             p.data_overage_cost_per_gb, 
#             p.text_limit_msg, 
#             p.text_overage_cost_per_msg
#         from 
#             raw r join 
#                 plan p on (r.plan_id = p.plan_id)
#         where r.is_master = 1 
#     ) /* select * from master order by rand() limit 10 */
#     select * from master
#     cluster by account_id
# """
# log.info("Creating master_line temp table.")
# master_line = spark.sql(master_line_sql)
# master_line.show()
# master_line.createOrReplaceTempView("master_line")
# log.warning("Master Line count: {}".format(master_line.count()))
# 
# additional_line_sql = """
#     with additional as (
#         select distinct 
#             r.account_id, 
#             r.line,
#             r.is_master,
#             r.foundation_id,
#             p.plan_name, 
#             p.plan_desc, 
#             p.base_cost, 
#             p.line_cost, 
#             p.rollover_voice_min, 
#             p.rollover_data_gb, 
#             p.adjustment_per_month, 
#             p.adjustment_length_month, 
#             p.voice_limit_min, 
#             p.voice_overage_cost_per_min, 
#             p.data_limit_gb, 
#             p.data_overage_cost_per_gb, 
#             p.text_limit_msg, 
#             p.text_overage_cost_per_msg
#         from 
#             raw r join 
#                 plan p on (r.plan_id = p.plan_id)
#         where r.is_master = 0 
#     ) /* select * from additional order by rand() limit 10 */
#     select * from additional 
#     cluster by account_id
# """
# log.info("Creating additional_line temp table.")
# additional_line = spark.sql(additional_line_sql)
# additional_line.show()
# additional_line.createOrReplaceTempView("additional_line")
# log.warning("Additional Line count: {}".format(additional_line.count()))
# 
# line_charge_sql = """
#     with all_line as (
#         select * from master_line union all
#         select * from additional_line
#     ) /* select * from all_line order by rand() limit 10 */
#     , plan_charge as (
#         -- only applied to the "master" line
#         select
#             m.plan_desc name,
#             m.base_cost charge,
#             m.line,
#             m.is_master,
#             m.account_id,
#             'monthly' grp1,
#             cast(NULL as string) grp2
#         from
#             master_line m
#     )
#     , master_line_monthly_charge as (
#         -- charges only for master lines
#         select
#             mc.name,
#             mc.value charge,
#             m.line,
#             m.is_master,
#             m.account_id,
#             'monthly' grp1,
#             cast(NULL as string) grp2
#         from
#             monthly_charges mc,
#             master_line m
#         where
#             mc.line_type in ('both', 'master')
#         
#     ) /* select * from master_line_monthly_charge order by rand() limit 10 */
#     , additional_line_monthly_charge as (
#         -- charges only for additional lines
#         select
#             mc.name,
#             mc.value charge,
#             a.line,
#             a.is_master,
#             a.account_id,
#             'monthly' grp1,
#             cast(NULL as string) grp2
#         from
#             monthly_charges mc,
#             additional_line a 
#         where
#             mc.line_type in ('both', 'master')
#         
#     ) /*select * from additional_line_monthly_charge order by rand() limit 10 */
#     , foundation_charge as (
#         -- master line only
#         select
#             f.name,
#             f.value charge,
#             m.line,
#             m.is_master,
#             m.account_id,
#             'monthly' grp1,
#             cast(NULL as string) grp2
#         from
#             foundation f inner join 
#                 master_line m on (f.foundation_id = m.foundation_id)
#     ) /* select * from foundation_charge order by rand() limit 10 */
#     , other_charge as (
#         select
#             oc.name,
#             oc.value charge,
#             al.line,
#             al.is_master,
#             al.account_id,
#             'other' grp1,
#             'fees' grp2
#         from
#             other_charges oc,
#             all_line al
#     ) /* select * from other_charge order by rand() limit 10 */
#     , fee_tax as (
#         select
#             ft.name,
#             ft.value charge,
#             al.line,
#             al.is_master,
#             al.account_id,
#             'other' grp1,
#             'taxes' grp2
#         from
#             fees_taxes ft,
#             all_line al
#     ) /* select * from other_charge order by rand() limit 10 */
#     , all_charge as (
#         select * from plan_charge union all
#         -- TODO: compute ratio based foundation charges
#         select * from foundation_charge union all
#         select * from master_line_monthly_charge union all
#         select * from additional_line_monthly_charge union all
#         select * from other_charge union all
#         select * from fee_tax 
#     )  /* select * from all_charge order by rand() limit 10 */
#     select * 
#     from all_charge 
#     --where account_id = '1001493346304' 
#     cluster by account_id
# """
# line_charge = spark.sql(line_charge_sql)
# line_charge.show(100)
# print("Row count: {}".format(line_charge.count()))

# TODO: Add logic to handle Adjustment logic

# voice usage per line
voice_usage_sql = """
    with a as (
        select
            r.account_id,
            r.plan_id,
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
            1, 2, 3, 4, 5, 6
    ) /* select * from a order by rand() limit 10 */
    , l as (
        select
            a.account_id,
            a.plan_id,
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
            l.line,
            l.curr_used,
            l.rollover,
            l.lim,
            calc_overage(l.curr_used, l.rollover, l.lim) overage,
            l.cost
        from l
    ) /* select * from o order by rand() limit 10 */
    select
        o.account_id,
        o.plan_id,
        o.line,
        o.curr_used,
        o.rollover,
        o.lim,
        round(o.overage * o.cost, 2) overage_cost
    from
        o
    where 
        o.overage > 0
    cluster by o.account_id
    --order by rand()
"""
voice_usage = spark.sql(voice_usage_sql)
voice_usage.createOrReplaceTempView("voice_usage")
voice_usage.show()

# data usage per line
data_usage_sql = """
    with a as (
        select
            r.account_id,
            r.plan_id,
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
            1, 2, 3, 4, 5, 6
    ) /* select * from a order by rand() limit 20 */
    , l as (
        select
            a.account_id,
            a.plan_id,
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
            l.line,
            l.curr_used,
            l.rollover,
            l.lim,
            calc_overage(l.curr_used, l.rollover, l.lim) overage,
            l.cost
        from l
    ) /* select * from o order by rand() limit 20 */
    select
        o.account_id,
        o.plan_id,
        o.line,
        o.curr_used,
        o.rollover,
        o.lim,
        round(o.overage * o.cost, 2) overage_cost
    from
        o
    where 
        o.overage > 0
    cluster by o.account_id
    --order by rand()
"""
data_usage = spark.sql(data_usage_sql)
data_usage.createOrReplaceTempView("data_usage")
data_usage.show()

