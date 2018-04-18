import sys
import math
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import settings

logging.basicConfig(level=logging.INFO, format=settings.FORMAT)
log = logging.getLogger(__name__)

spark = settings.SPARK

#########
# utility functions
def get_lookup(table):
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
			)
	except Exception as e:
		log.execption("Problem retreiving remote lookup table: {}".format(table))
	finally:
		return retval


#######
# spark functions
def value_or_ratio(value, ratio):
	return value if value is not None else ratio


def get_line(phone, line):
	return "master" if phone==line else "additional"


def calculate_message_fee(messages_used, text_limit_msg, text_overage_cost_per_msg):
    if messages_used is None or text_limit_msg is None or messages_used <= text_limit_msg:
        return 0
    else:
        return (messages_used - text_limit_msg) * text_overage_cost_per_msg


def get_base(line, base_cost, line_cost):
	return base_cost if line == "master" else line_cost

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

spark.udf.register("valueRatio", value_or_ratio)
spark.udf.register("getLine", get_line)
spark.udf.register("getBase", get_base)
spark.udf.register("calculateMessageFee", calculate_message_fee)


if __name__ == '__main__':

	# get lookup tables
	PLANS = get_lookup("public.plan").load().cache()
	PLANS.createOrReplaceTempView("plans")

	# build combined list of line-level fees and monthly charges
	_OTHER_CHARGES = get_lookup("public.other_charges").load()
	_MONTHLY_CHARGES = get_lookup("public.monthly_charges").load()
	_FEES = get_lookup("public.fees_taxes").load()
	CHARGES = _OTHER_CHARGES.unionAll(_FEES).unionAll(_MONTHLY_CHARGES).cache()
	CHARGES.createOrReplaceTempView("charges")

	# foundation account lookup
	FOUNDATION = get_lookup("public.foundation").load().cache()
	FOUNDATION.createOrReplaceTempView("foundation")
	
	raw_data = get_raw_data(settings.RAW_FILE, settings.RAW_SCHEMA)

	txns = raw_data

