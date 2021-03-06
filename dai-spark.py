
from pyspark.sql import SparkSession
from pyspark.sql.types import *


import sys

def read_file(my_file):
    with open(my_file, 'r') as myfile:
        data=myfile.read()
    return data


spark = SparkSession.builder.appName("Spark DAI POC").getOrCreate()

source_trx_data = sys.argv[1]
source_sql_schema_file = sys.argv[2]
sql_file = sys.argv[3]
lineOutput = sys.argv[4]
source_plan_data = sys.argv[5]
source_plan_schema=sys.argv[6]
calculation_sql_file = sys.argv[7]
calculation_summary_sql_file = sys.argv[8]
calculation_usage_sql_file = sys.argv[9]
other_charge_data = sys.argv[10]
other_charge_schema = sys.argv[11]
line_charge_data = sys.argv[12]
line_charge_schema = sys.argv[13]
accountOutput = sys.argv[14]
trxOutput = sys.argv[15]

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

def calculateMessageFee(messages_used,text_limit_msg,text_overage_cost_per_msg):
    messages_used = int(messages_used)
    text_limit_msg = int(text_limit_msg)
    text_overage_cost_per_msg = float(text_overage_cost_per_msg)
    if messages_used > text_limit_msg:
        return (messages_used - text_limit_msg)*text_overage_cost_per_msg
    else:
        return 0

def getBase(line,base_cost, line_cost):
    if line =="master":
        return base_cost
    else:
        return line_cost

spark.udf.register("valueRatio", valueRatio)
spark.udf.register("getLine", getLine)
spark.udf.register("getBase", getBase)
spark.udf.register("calculateMessageFee", calculateMessageFee)

#Source Transaction_data
source_sql_rdd = spark.sparkContext.textFile(source_trx_data)

sc_rowRDD = source_sql_rdd.map(lambda l: l.split(","))

source_sql_schema_data = read_file(source_sql_schema_file)

fields_1 = [StructField(field_name, StringType(), True) for field_name in source_sql_schema_data.split(",")]

schema = StructType(fields_1)

source_sql_df = spark.createDataFrame(sc_rowRDD, schema).cache()

source_sql_df.createOrReplaceTempView("trx_table")

# results=spark.sql("select * from trx_table").show(5)

#Source Plan_data

plan_sql_rdd = spark.sparkContext.textFile(source_plan_data)

plan_rowRDD = plan_sql_rdd.map(lambda l: l.split(","))

plan_schema_data = read_file(source_plan_schema)

plan_fields = [StructField(field_name, StringType(), True) for field_name in plan_schema_data.split(",")]

plan_schema = StructType(plan_fields)

plan_df = spark.createDataFrame(plan_rowRDD, plan_schema)

plan_df.createOrReplaceTempView("plan_table")

plan_results=spark.sql("select * from plan_table").show(5)

#Load Sql logic
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

account_usage.createOrReplaceTempView("account_usage")

calculation_usage_sql = read_file(calculation_usage_sql_file)

account_usage_cost = spark.sql(calculation_usage_sql).cache()

account_usage_cost.show(50)

account_usage_cost.createOrReplaceTempView("account_usage_cost")


#Source Charge Data
other_charge_rdd = spark.sparkContext.textFile(other_charge_data)

other_charge_rowRDD = other_charge_rdd.map(lambda l: l.split(","))

other_charge_schema_data = read_file(other_charge_schema)

charge_schema_fields = [StructField(field_name, StringType(), True) for field_name in other_charge_schema_data.split(",")]

charge_schema = StructType(charge_schema_fields)

charge_df = spark.createDataFrame(other_charge_rowRDD, charge_schema)

charge_df.createOrReplaceTempView("other_charge")

spark.sql("select * from other_charge").show(10)

lineOthers = spark.sql("select a.account_id , a.plan_id, a.line, a.lineInfo, b.name, b.value from account_line_agg a cross join other_charge b")

#Line Charges

line_charge_rdd = spark.sparkContext.textFile(line_charge_data)

line_charge_rowRDD = line_charge_rdd.map(lambda l: l.split(","))

line_charge_schema_data = read_file(line_charge_schema)

line_charge_schema_fields = [StructField(field_name, StringType(), True) for field_name in line_charge_schema_data.split(",")]

line_charge_schema = StructType(line_charge_schema_fields)

charge_df = spark.createDataFrame(line_charge_rowRDD, line_charge_schema)

charge_df.createOrReplaceTempView("line_charge")

spark.sql("select * from line_charge").show(10)

line1 = spark.sql("select a.account_id , a.plan_id, a.line, a.lineInfo, b.name, b.value from  account_line_agg a  inner join line_charge b where a.lineInfo = b.line_type")
line2 = spark.sql("select a.account_id , a.plan_id, a.line, a.lineInfo,b.name, b.value from  account_line_agg a cross join line_charge b where b.line_type = 'both' ")

#schema account_id| plan_id|line|  lineInfo|  name|value|
totalLine= line1.unionAll(line2).unionAll(lineOthers).repartition(1)
totalLine.registerTempTable("totalLine_Charge")
#ACCOUNT LINE output
totalLine.write.csv(lineOutput, mode='overwrite')

#ACCOUNT output
account_total = spark.sql("select a.account_id, a.plan_id, a.msg_cost,voice_cost, a.data_cost, a.prev_balance, -a.prev_balance as prev_payment,a.adjustments,0 as balance,"
          "(a.adjustments + a.total+b.line_total) as new_charges,  (a.adjustments + a.total+b.line_total) as debit_amount"
          " from account_usage_cost a inner join "
          "( select c.account_id, c.plan_id, sum(c.value) as line_total from totalLine_Charge c group by account_id, plan_id) b on "
          "a.account_id = b.account_id and a.plan_id = b.plan_id")

account_total.show(5)

account_total.write.csv(accountOutput, mode='overwrite')

#Transaction_output
trx = spark.sql ("select account_id,plan_id,line, txn_type, place,sent_recv ,to_from, in_plan, in_network,mins,type_unit from trx_table ")
trx.show(5)

trx.write.csv(trxOutput, mode='overwrite')
