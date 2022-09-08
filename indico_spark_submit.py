"""
Submits files to a workflow
"""

#spark-submit --driver-class-path ../../Drivers/postgres/postgresql-42.2.6.jar --packages io.delta:delta-core_2.12:2.0.0 indico_spark.py
#spark-submit --packages io.delta:delta-core_2.12:2.0.0 indico_spark.py
from __future__ import absolute_import
from pyspark.sql import SparkSession
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
import argparse
import glob
import time
import datetime
import pandas as pd
import logging
import importlib
import os
import sys
import requests
import indico
timestr = time.strftime("%Y%m%d-%H%M%S")
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
from indico import IndicoClient, IndicoConfig
from indico.filters import SubmissionFilter, or_
from indico.queries import (
    JobStatus,
    ListSubmissions,
    RetrieveStorageObject,
    SubmissionResult,
    SubmitReview,
    UpdateSubmission,
    WaitForSubmissions,
    WorkflowSubmission,
    WorkflowSubmissionDetailed,
    GraphQLRequest
)

from pyspark.sql.functions import *
import json
from pyspark.sql.types import *
API_TOKEN = str(os.environ['INDICO_TOKEN'])
my_config = IndicoConfig(
    host='try.indico.io',
api_token=API_TOKEN
)
client = IndicoClient(config=my_config)
delta_path = "./delta/"

def read_delta_table(table_name,spark):
  df = spark.read.format("delta").load(delta_path+table_name)
  return df

def make_delta_table(df,table_name,partion_column):
  df.write\
      .format("delta")\
      .option("delta.columnMapping.mode", "name")\
      .mode("overwrite")\
      .option("overwriteSchema", "true")\
      .option("mergeSchema", "true")\
      .partitionBy("{0}".format(partion_column))\
      .save(delta_path+table_name)
  #.option("mergeSchema", "true")
  return "Success"

def add_current_date(df):
    df = df.withColumn("Run Date", current_date())
    return df

def submit_files_to_workflow(workflow_id, directory):
    files = glob.glob(directory)
    submission_ids = client.call(WorkflowSubmission(workflow_id=workflow_id, files=files))
    return submission_ids

################################################################################################
################################ RUN JOB BELOW #################################################
################################################################################################
def run(argv=None):

    # TODO: Update job to run with workflow ID arg

    parser = argparse.ArgumentParser()
    parser.add_argument('--workflow_id', dest='workflow_id',help='Workflow ID')
    parser.add_argument('--directory', dest='directory', help="location of test files")

    known_args = parser.parse_args()

    spark = SparkSession.builder.appName("Indico Spark Extraction").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    databaseName = "IndicoDatabase"
    spark.sql("CREATE DATABASE IF NOT EXISTS `{}`".format(databaseName))
    spark.sql("USE `{}`".format(databaseName))

    #Submit files to indico and get list
    files = glob.glob(known_args.directory)
    submission_ids = submit_files_to_workflow(int(known_args.workflow_id), known_args.directory)
    print(submission_ids)

  
    spark.stop()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()