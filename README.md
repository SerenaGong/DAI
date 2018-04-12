# DAI

spark-submit --driver-class-path /Users/sgong/PycharmProjects/DAI/DAI/postgresql-42.2.2.jar /Users/sgong/PycharmProjects/DAI/DAI/dai-spark-postgres.py /Users/sgong/PycharmProjects/DAI/DAI/DAI_data/test.csv /Users/sgong/PycharmProjects/DAI/DAI/DAI_schema/trx.csv /Users/sgong/PycharmProjects/DAI/DAI/DAI_sql/join.sql /Users/sgong/PycharmProjects/DAI/DAI/DAI_output /Users/sgong/PycharmProjects/DAI/DAI/DAI_sql/calculation.sql /Users/sgong/PycharmProjects/DAI/DAI/DAI_sql/calculation_summary.sql /Users/sgong/PycharmProjects/DAI/DAI/DAI_sql/calculation_usage.sql /Users/sgong/PycharmProjects/DAI/DAI/DAI_accountOutput /Users/sgong/PycharmProjects/DAI/DAI/DAI_trxOutput

##Run Cluster Mode

/usr/hdp/current/spark2-client/bin/spark-submit  --jars /root/code/postgresql-42.2.2.jar --driver-class-path /root/code/postgresql-42.2.2.jar --master yarn --deploy-mode client --driver-memory 1g --executor-memory 2g /root/code/dai-spark-postgres.py /user/serena/hwpoc_mock_100.csv.gz /root/code/trx.csv /root/code/join.sql /user/serena/output /root/code/calculation.sql /root/code/calculation_summary.sql /root/code/calculation_usage.sql /user/serena/accountOutput /user/serena/trxOutput