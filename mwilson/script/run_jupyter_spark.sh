#!/bin/sh

CURR_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYSPARK_DRIVER_PYTHON=jupyter 
export PYSPARK_DRIVER_PYTHON_OPTS='notebook' 
export SPARK_HOME=/Users/WendyJiang/.pyenv/versions/3.6.4/envs/hwpoc/lib/python3.6/site-packages/pyspark 
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_162.jdk/Contents/Home 

JDBC_JAR_PATH=$CURR_PATH/../lib/postgresql-42.2.2.jar

pyspark --master "local[4]" --driver-memory 1g --executor-memory 1g --executor-cores 2 --driver-class-path $JDBC_JAR_PATH 

