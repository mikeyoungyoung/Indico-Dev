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
import time
import datetime
import pandas as pd
import logging
import os
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
import pandas as pd
from pyspark.sql.functions import *
import json
from pyspark.sql.types import *
# In databricks, this is how to get the token: API_TOKEN = dbutils.secrets.get(scope = "indico", key = "api_token")
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
      .mode("append")\
      .option("overwriteSchema", "true")\
      .partitionBy("{0}".format(partion_column))\
      .save(delta_path+table_name)
  return "Success"

def add_current_date(df):
    df = df.withColumn("Run Date", current_date())
    return df

################################################################################################
################################ RUN JOB BELOW #################################################
################################################################################################
def run(argv=None):

    # TODO: Update job to run with workflow ID arg??

    parser = argparse.ArgumentParser()
    parser.add_argument('--workflow_id',dest='workflow_id',help='Workflow ID')

    known_args = parser.parse_args()

    spark = SparkSession.builder.appName("Indico Spark Extraction").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    databaseName = "IndicoDatabase"
    spark.sql("CREATE DATABASE IF NOT EXISTS `{}`".format(databaseName))
    spark.sql("USE `{}`".format(databaseName))
  # GraphQL Query to list my datasets
    dataset_qstr = """{
            datasets {
                id
                name
                status
                rowCount
                numModelGroups
                modelGroups {
                    id
                }
            }
        }"""
    
    #submissions query
    submissions_query ="""
query ListSubmissions(
            $submissionIds: [Int],
            $workflowIds: [Int],
            $filters: SubmissionFilter,
            $limit: Int,
            $orderBy: SUBMISSION_COLUMN_ENUM,
            $desc: Boolean,
            $after: Int

        ){
            submissions(
                submissionIds: $submissionIds,
                workflowIds: $workflowIds,
                filters: $filters,
                limit: $limit
                orderBy: $orderBy,
                desc: $desc,
                after: $after

            ){
                submissions {
                    id
                    datasetId
                    workflowId
                    status
                    inputFile
                    inputFilename
                    resultFile
                    deleted
                    retrieved
                    errors
                    reviews {
                        id
                        createdAt
                        createdBy
                        completedAt
                        rejected
                        reviewType
                        notes
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
"""
    submissions_response = client.call(GraphQLRequest(query=submissions_query))
    
    #Make Delta Tables
    datasets_response = client.call(GraphQLRequest(query=dataset_qstr))
    #Create spark datafram from datasets_response list
    datasets = spark.createDataFrame(datasets_response['datasets'])
    datasets.printSchema()
    #write datasets df to a delta table
    make_delta_table(datasets,"indico_datasets", "id")

    # make submissions table from pandas
    subs_pdf = pd.DataFrame.from_dict(submissions_response['submissions']['submissions'])
    print(subs_pdf.dtypes)
    # Filter to only one workflow for the results, probably better to do this in the Indico request
    # Initial goal was to grab all submissions into a single dataframe
    # It only became a problem when I was trying to get the results as a column in a single df
    subs_pdf = subs_pdf[subs_pdf.workflowId.eq(701)]
    
    # We can probably dynamically build this schema
    submissiomns_schema = StructType([ \
        StructField("id",IntegerType(),True), \
        StructField("datasetId",IntegerType(),True), \
        StructField("workflowId",IntegerType(),True), \
        StructField("status", StringType(), True), \
        StructField("inputFile", StringType(), True), \
        StructField("inputFilename", StringType(), True), \
        StructField("resultFile", StringType(), True), \
        StructField("deleted", BooleanType(), True), \
        StructField("retrieved", BooleanType(), True), \
        StructField("errors", StringType(), True), \
        StructField("reviews", StringType(), True)
    ])
    submissions_df = spark.createDataFrame(subs_pdf, schema=submissiomns_schema)
    
    # Data hygiene prior to getting results
    # I was trying to get results from deleted files that were erroring out

    submissions_df.printSchema()
    submissions_df.groupBy('deleted').count().show()
    submissions_df = submissions_df.filter(submissions_df.deleted == False)
    submissions_df.groupBy('deleted').count().show()
    submissions_df.show()
    submissions_df = submissions_df.filter(submissions_df.resultFile.isNotNull())
    
    # UDF to get results as a json string column 
    def get_results(path):
        result = client.call(RetrieveStorageObject(path))
        return json.dumps(result)
    resultsUDF = udf(lambda z: get_results(z),StringType())

    complete_with_results = submissions_df\
        .withColumn("Results", resultsUDF(submissions_df.resultFile).alias("ResultsString"))\
        .withColumn("Run_Date", current_date()) # add current date for snapshot purposes if getting results more than once
    complete_with_results.printSchema()
    complete_with_results.show(5)
    
    print("Attempt to write deltatable")
    try:
        make_delta_table(complete_with_results,"indico_results_bronze","id")
    except Exception as E:
        print("------------------- FAILED TO CREATE DELTA TABLE -------------------")
        print(E)
        print("------------------- Writing Parquet File -------------------")
        complete_with_results.write.mode('overwrite').parquet('./complete_results_raw')


    spark.stop()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()