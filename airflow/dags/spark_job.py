import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types
import argparse
import os

SERVICE_ACCOUNT_JSON_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
BQ_DATASET_PROD = os.environ.get('BIGQUERY_DATASET', 'ticketmaster_prod')
TMP_BUCKET = os.environ.get('TMP_BUCKET')

SPARK_GCS_JAR = "/opt/airflow/lib/gcs-connector-hadoop3-2.2.5.jar"
SPARK_BQ_JAR = "/opt/airflow/lib/spark-bigquery-latest_2.12.jar"


schema = types.StructType(

    [
        types.StructField("name", types.StringType(), True),
        types.StructField("type", types.StringType(), True),
        types.StructField("id", types.StringType(), True),
        types.StructField("url", types.StringType(), True),
        types.StructField("locale", types.StringType(), True),
        types.StructField("images", types.StringType(), True),
        types.StructField("sales", types.StringType(), True),
        types.StructField("dates", types.StringType(), True),
        types.StructField("classifications", types.StringType(), True),
        types.StructField("promoter", types.StringType(), True),
        types.StructField("promoters", types.StringType(), True),
        types.StructField("priceRanges", types.StringType(), True),
        types.StructField("_links", types.StringType(), True),
        types.StructField("_embedded", types.StringType(), True),
        types.StructField("seatmap", types.StringType(), True),
    ]
)

parser = argparse.ArgumentParser()
parser.add_argument('--input_file', required=True)

args = parser.parse_args()
input_file = args.input_file

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", f"{SPARK_GCS_JAR},{SPARK_BQ_JAR}") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT_JSON_PATH) \
    .set('temporaryGcsBucket', TMP_BUCKET) \
    .set("viewsEnabled","true") \
    .set("materializationDataset",f"{BQ_DATASET_PROD}")

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set(
    "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile",
                SERVICE_ACCOUNT_JSON_PATH)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()


df = (
    spark.read.option("header", "true").schema(schema).csv(input_file)
)
df.show()
df.printSchema()

df.createOrReplaceTempView("enrich_full_data")


df.write.format('bigquery') \
    .option('table', f"{BQ_DATASET_PROD}.raw_data") \
    .mode('overwrite') \
    .save()