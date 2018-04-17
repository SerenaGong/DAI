#!/bin/sh

CURR_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYSPARK_DRIVER_PYTHON='ipython'
unset PYSPARK_DRIVER_PYTHON_OPTS
export SPARK_HOME=/Users/michael.wilson/.pyenv/versions/3.6.4/envs/hwpoc-serena/lib/python3.6/site-packages/pyspark
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_162.jdk/Contents/Home 

pyspark --driver-class-path $CURR_PATH/../lib/postgresql-42.2.2.jar
