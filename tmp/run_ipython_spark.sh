#!/bin/sh
export PYSPARK_DRIVER_PYTHON=ipython
export SPARK_HOME=/Users/michael.wilson/.pyenv/versions/3.6.4/envs/hwpoc-serena/lib/python3.6/site-packages/pyspark
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_162.jdk/Contents/Home
pyspark
