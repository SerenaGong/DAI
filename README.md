# DAI

spark-submit --driver-class-path /Users/sgong/PycharmProjects/sparkTest/postgresql-42.2.2.jar /Users/sgong/PycharmProjects/sparkTest/dai-spark-postgres.py /Users/sgong/PycharmProjects/sparkTest/DAI_data/test.csv /Users/sgong/PycharmProjects/sparkTest/DAI_schema/trx.csv /Users/sgong/PycharmProjects/sparkTest/DAI_sql/join.sql /Users/sgong/PycharmProjects/sparkTest/DAI_output /Users/sgong/PycharmProjects/sparkTest/DAI_sql/calculation.sql /Users/sgong/PycharmProjects/sparkTest/DAI_sql/calculation_summary.sql /Users/sgong/PycharmProjects/sparkTest/DAI_sql/calculation_usage.sql /Users/sgong/PycharmProjects/sparkTest/DAI_accountOutput /Users/sgong/PycharmProjects/sparkTest/DAI_trxOutput