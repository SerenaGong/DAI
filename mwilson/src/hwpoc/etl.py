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


# register udfs for access by spark sql
spark.udf.register("valueRatio", value_or_ratio)
spark.udf.register("is_master", is_master)
spark.udf.register("getBase", get_base)
spark.udf.register("calculateMessageFee", calculate_message_fee)


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
raw.show(5)

# create distinct members set
members = raw.select(*settings.MEMBER_FIELDS).distinct()
members.createOrReplaceTempView("members")
members.show(5)

txns = raw.select(*settings.TXN_FIELDS)
txns.createOrReplaceTempView("txns")
txns.show(5)
  
# skipping per plan logic, assuming all members are in PLAN3000 for now.
log.warning("Assuming all members are in PLAN3000 (for now)")

# TODO: figure out why removing address_1 for some members produces
# empty dataframes.

# calc line-level detail charges
query = """
    with master as (
        select distinct 
            t.account_id, 
            t.line,
            t.is_master,
            t.foundation_id,
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
            txns t join 
                plan p on (t.plan_id = p.plan_id)
        where t.is_master = 1 
    ) /* select * from master order by rand() limit 10 */
    , master_charge as (
        select
            m.plan_desc name,
            m.base_cost charge,
            m.line,
            m.is_master,
            m.account_id,
            'monthly' grp1,
            cast(NULL as string) grp2
        from
            master m
    )
    , monthly_charge as (
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
            master m
        where
            mc.line_type in ('both', 'master')
        
    ) /* select * from monthly_charges order by rand() limit 10 */
    , foundation_charge as (
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
                master m on (f.foundation_id = m.foundation_id)
    ) /* select * from foundation_charge order by rand() limit 10 */
    , other_charge as (
        select
            oc.name,
            oc.value charge,
            m.line,
            m.is_master,
            m.account_id,
            'other' grp1,
            'fees' grp2
        from
            other_charges oc,
            master m
    ) /* select * from other_charge order by rand() limit 10 */
    , fee_tax as (
        select
            ft.name,
            ft.value charge,
            m.line,
            m.is_master,
            m.account_id,
            'other' grp1,
            'taxes' grp2
        from
            fees_taxes ft,
            master m
    ) /* select * from other_charge order by rand() limit 10 */
    , all_charge as (
        select * from master_charge union all
        -- TODO: compute ratio based foundation charges
        select * from foundation_charge union all
        select * from monthly_charge union all
        select * from other_charge union all
        select * from fee_tax
    )  /* select * from all_charge order by rand() limit 10 */
    select * 
    from all_charge 
    where account_id = '1001493346304' 
    order by account_id
"""
result = spark.sql(query)
result.show(100)

