## Spark 

#####Run Local Mode Command
```python
spark-submit --driver-class-path /Users/sgong/PycharmProjects/DAI/DAI/postgresql-42.2.2.jar /Users/sgong/PycharmProjects/DAI/DAI/dai-spark-postgres.py /Users/sgong/PycharmProjects/DAI/DAI/DAI_data/test.csv /Users/sgong/PycharmProjects/DAI/DAI/DAI_schema/trx.csv /Users/sgong/PycharmProjects/DAI/DAI/DAI_sql/join.sql /Users/sgong/PycharmProjects/DAI/DAI/DAI_output /Users/sgong/PycharmProjects/DAI/DAI/DAI_sql/calculation.sql /Users/sgong/PycharmProjects/DAI/DAI/DAI_sql/calculation_summary.sql /Users/sgong/PycharmProjects/DAI/DAI/DAI_sql/calculation_usage.sql /Users/sgong/PycharmProjects/DAI/DAI/DAI_accountOutput /Users/sgong/PycharmProjects/DAI/DAI/DAI_trxOutput
```

#####Run Cluster Mode Command
```python
/usr/hdp/current/spark2-client/bin/spark-submit  --jars /root/code/postgresql-42.2.2.jar --driver-class-path /root/code/postgresql-42.2.2.jar --master yarn --deploy-mode client  /root/code/dai-spark-postgres.py /user/serena/hwpoc_mock_100.csv.gz /root/code/trx.csv /root/code/join.sql /user/serena/output /root/code/calculation.sql /root/code/calculation_summary.sql /root/code/calculation_usage.sql /user/serena/accountOutput /user/serena/trxOutput
```

#####Spark Output Requirements 
Customer is looking account summary, account line, transaction files have same partition, and each account's data will be in the same partition number for account, account_line, and transaction files. 
Solution: Having pmod function apply on account_id which will be the partition key column. Repartition the dataframe within the SQL and write data frames to files. 

#####Spark Properties Configure

Need to adjust Spark properties according to the real dataset and cluster environment. 

```python
spark = SparkSession.builder\
    .config("spark.executor.memory", "1g")\
    .config("spark.default.parallelism", "4")\
    .config("spark.sql.shuffle.partitions", "4") \
    .appName("Spark DAI POC").getOrCreate()
```


## NiFi 





## Deploy Spark Job
Jenkin job read source from git repo, deploy the code to EdgeNode. On all EdgeNodes, have already installed Spark clients which will have yarn, spark master info.
