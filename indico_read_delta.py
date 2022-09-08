"""
Read the data saved to a delta table in the indico_spark_get_results.py script
"""

#spark-submit --driver-class-path ../../Drivers/postgres/postgresql-42.2.6.jar --packages io.delta:delta-core_2.12:2.0.0 indico_spark.py
#spark-submit --packages io.delta:delta-core_2.12:2.0.0 indico_spark.py
from __future__ import absolute_import
from asyncore import read
from pyspark.sql import SparkSession
from pyspark.sql import Row
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
from pyspark.sql import Window
from pyspark.sql.functions import *
import argparse
import time
import datetime
import pandas as pd
import logging

def make_delta_table(df,table_name,partion_column):
  df.write\
      .format("delta")\
      .option("delta.columnMapping.mode", "name")\
      .mode("append")\
      .option("overwriteSchema", "true")\
      .partitionBy("{0}".format(partion_column))\
      .save(delta_path+table_name)
  return "Success"

def read_delta_table(table_name,spark):
  df = spark.read.format("delta").load(delta_path+table_name)
  return df
delta_path = "./delta/"
spark = SparkSession.builder.appName("Spark Pricer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
databaseName = "IndicoDatabase"
spark.sql("CREATE DATABASE IF NOT EXISTS `{}`".format(databaseName))
spark.sql("USE `{}`".format(databaseName))  

datasets = read_delta_table("indico_datasets",spark)
submissions = read_delta_table("indico_results_bronze",spark)
# datasets.printSchema()
# datasets.show(2)
# submissions.printSchema()
submissions = submissions.withColumn(
    "ParsedResults", get_json_object(col("Results"), 
    "$.results.document.results").alias("results"))
json_schema = spark.read.json(submissions.rdd.map(lambda row: row.ParsedResults)).schema
results_df = submissions.withColumn('results_struct', from_json(col('ParsedResults'), json_schema))
# results_df.show(1)
# results_df.printSchema()
make_delta_table(results_df,"indico_results_silver","id")