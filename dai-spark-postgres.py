
from pyspark.sql import SparkSession
from pyspark.sql.types import *


import sys

def read_file(my_file):
    with open(my_file, 'r') as myfile:
        data=myfile.read()
    return data


spark = SparkSession.builder\
    .config("spark.executor.memory", "1g")\
    .config("spark.default.parallelism", "4")\
    .config("spark.sql.shuffle.partitions", "4") \
    .appName("Spark DAI POC").getOrCreate()

source_trx_data = sys.argv[1]
source_sql_schema_file = sys.argv[2]
sql_file = sys.argv[3]
lineOutput = sys.argv[4]
calculation_sql_file = sys.argv[5]
calculation_summary_sql_file = sys.argv[6]
calculation_usage_sql_file = sys.argv[7]
accountOutput = sys.argv[8]
trxOutput = sys.argv[9]



print("sys.argv[1] = source_trx_data = {}".format(source_trx_data))
print("sys.argv[2] = source_sql_schema_file = {}".format(source_sql_schema_file))
print("sys.argv[3] = sql_file = {}".format(sql_file))
print("sys.argv[4] = lineOutput = {}".format(lineOutput))
print("sys.argv[5] = calculation_sql_file = {}".format(calculation_sql_file))
print("sys.argv[6] = calculation_summary_sql_file = {}".format(calculation_summary_sql_file))
print("sys.argv[7] = calculation_usage_sql_file = {}".format(calculation_usage_sql_file))
print("sys.argv[8] = accountOutput = {}".format(accountOutput))
print("sys.argv[9] = trxOutput = {}".format(trxOutput))

def valueRatio(value, ratio):
    if value is not None:
        return value
    else:
        return ratio

def getLine(phone,line):
    if phone==line:
        return "master"
    else:
        return "additional"

def calculateMessageFee(messages_used, text_limit_msg, text_overage_cost_per_msg):
    if messages_used is None or text_limit_msg is None or messages_used <= text_limit_msg:
        return 0
    else:
        return (messages_used - text_limit_msg) * text_overage_cost_per_msg

def getBase(line,base_cost, line_cost):
    if line =="master":
        return base_cost
    else:
        return line_cost

spark.udf.register("valueRatio", valueRatio)
spark.udf.register("getLine", getLine)
spark.udf.register("getBase", getBase)
spark.udf.register("calculateMessageFee", calculateMessageFee)

src_jdbc_conn_str="jdbc:postgresql://hwdb1.cjbwf6taixqt.us-east-1.rds.amazonaws.com:5432/hwpoc"

#src_jdbc_conn_str="jdbc:postgresql://localhost:5432/postgres"

username="hwpoc"
password="hwpoc"

#Source Transaction_data
source_sql_rdd = spark.sparkContext.textFile(source_trx_data)

sc_rowRDD = source_sql_rdd.map(lambda l: l.split(","))

source_sql_schema_data = read_file(source_sql_schema_file)

fields_1 = [StructField(field_name, StringType(), True) for field_name in source_sql_schema_data.split(",")]

schema = StructType(fields_1)

source_sql_df = spark.createDataFrame(sc_rowRDD, schema).na.fill(0).cache()


source_sql_df.createOrReplaceTempView("trx_table")

spark.sql("select * from trx_table").show(5)


#Testing to have 10 Partition files
spark.sql("select distinct pmod(account_id,10) from trx_table").show(10)




plan_df = spark.read.format('jdbc').options(url=src_jdbc_conn_str,
             driver="org.postgresql.Driver",
             user=username,
             password=password,
             dbtable="plan").load().na.fill(0).cache()

plan_df.printSchema()

plan_df.createOrReplaceTempView("plan_table")

plan_results=spark.sql("select * from plan_table").show(5)

# #Load Sql logic
curation_sql = read_file(sql_file)

account_line_txn_type_agg = spark.sql(curation_sql).cache()

account_line_txn_type_agg.show(50)

account_line_txn_type_agg.createOrReplaceTempView("account_line_txn_type_agg")

calculation_sql = read_file(calculation_sql_file)

account_line_agg = spark.sql(calculation_sql).cache()

account_line_agg.show(50)

account_line_agg.createOrReplaceTempView("account_line_agg")

calculation_summary_sql = read_file(calculation_summary_sql_file)

account_usage = spark.sql(calculation_summary_sql)

account_usage.show(50)

account_usage.printSchema()

account_usage.createOrReplaceTempView("account_usage")

calculation_usage_sql = read_file(calculation_usage_sql_file)

account_usage_cost = spark.sql(calculation_usage_sql).cache()

account_usage_cost.show(50)

account_usage_cost.createOrReplaceTempView("account_usage_cost")


#Source Charge Data
charge_df = spark.read.format('jdbc').options(url=src_jdbc_conn_str,
             driver="org.postgresql.Driver",
             user=username,
             password=password,
             dbtable="other_charges").load()


#Source fees_taxes

fees_taxes_df = spark.read.format('jdbc').options(url=src_jdbc_conn_str,
             driver="org.postgresql.Driver",
             user=username,
             password=password,
             dbtable="fees_taxes").load()

all_others_charges=charge_df.unionAll(fees_taxes_df).cache()

all_others_charges.createOrReplaceTempView("all_others_charges")

spark.sql("select * from all_others_charges").show(10)

lineOthers = spark.sql("select a.account_id , a.plan_id, a.line, a.lineInfo, b.name, b.value from account_line_agg a cross join all_others_charges b")

#Line Charges

line_charge_df = spark.read.format('jdbc').options(url=src_jdbc_conn_str,
             driver="org.postgresql.Driver",
             user=username,
             password=password,
             dbtable="monthly_charges").load().cache()

line_charge_df.createOrReplaceTempView("line_charge")

spark.sql("select * from line_charge").show(10)

line1 = spark.sql("select a.account_id , a.plan_id, a.line, a.lineInfo, b.name, b.value from  account_line_agg a  inner join line_charge b where a.lineInfo = b.line_type")
line2 = spark.sql("select a.account_id , a.plan_id, a.line, a.lineInfo,b.name, b.value from  account_line_agg a cross join line_charge b where b.line_type = 'both' ")

#schema account_id| plan_id|line|  lineInfo|  name|value|
totalLine= line1.unionAll(line2).unionAll(lineOthers)
totalLine.registerTempTable("totalLine_Charge")

totalLine2 = spark.sql("select * from totalLine_Charge DISTRIBUTE BY pmod(account_id,10)")
#ACCOUNT LINE output
totalLine2.write.csv(lineOutput, mode='overwrite')

#ACCOUNT output
account_total = spark.sql("select a.account_id, a.plan_id, a.msg_cost,voice_cost, a.data_cost, cast(a.prev_balance as int), -cast(a.prev_balance as int) as prev_payment,a.adjustments,0 as balance,"
          "(cast( a.adjustments  as float) + a.total + b.line_total) as new_charges,  (cast(a.adjustments as float) + a.total+b.line_total) as debit_amount"
          " from account_usage_cost a inner join "
          "( select c.account_id, c.plan_id, sum(c.value) as line_total from totalLine_Charge c group by account_id, plan_id) b on "
          "a.account_id = b.account_id and a.plan_id = b.plan_id DISTRIBUTE BY pmod(account_id,10)")

account_total.show(5)

account_total.write.csv(accountOutput, mode='overwrite')

#Transaction_output
trx = spark.sql ("select account_id,plan_id,line, txn_type, place,sent_recv ,to_from, in_plan, in_network,mins,type_unit from trx_table DISTRIBUTE BY pmod(account_id,10)")
trx.show(5)

trx.write.csv(trxOutput, mode='overwrite')