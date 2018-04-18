import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *

FORMAT='%(asctime)s %(levelname)s %(module)s:%(funcName)s >> %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

PG_URL="jdbc:postgresql://hwdb1.cjbwf6taixqt.us-east-1.rds.amazonaws.com:5432/hwpoc"
SPARK = (SparkSession.builder.appName('hwpoc').enableHiveSupport().getOrCreate())

log.info("Spark Context: {}".format(SPARK.sparkContext._conf.getAll()))

# input datafile & schema
RAW_FILE = "/Users/michael.wilson/build/DAI/mwilson/fixtures/test.csv"
RAW_SCHEMA = StructType([
    StructField('account_id', LongType(), False),
    StructField('last_name', StringType(), False),
    StructField('first_name', StringType(), False),
    StructField('phone', StringType(), False),
    StructField('address_1', StringType(), False),
    StructField('address_2', StringType(), True),
    StructField('city', StringType(), False),
    StructField('state', StringType(), False),
    StructField('postal_code', StringType(), False),
    StructField('plan_id', StringType(), False),
    StructField('foundation_id', StringType(), True),
    StructField('joined_at', TimestampType(), False),
    StructField('prev_balance', FloatType(), True),
    StructField('adjustments', FloatType(), True),
    StructField('prev_voice', IntegerType(), True),
    StructField('prev_data', IntegerType(), True),
    StructField('line', StringType(), False),
    StructField('txn_type', StringType(), False),
    StructField('txn_at', StringType(), False),
    StructField('place', StringType(), True),
    StructField('sent_recv', StringType(), True),
    StructField('to_from', StringType(), True),
    StructField('in_plan', IntegerType(), True),
    StructField('in_network', IntegerType(), True),
    StructField('mins', IntegerType(), True),
    StructField('type_unit', StringType(), True)
])


