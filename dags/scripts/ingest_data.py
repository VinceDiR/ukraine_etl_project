""""ACLED Data Ingest Function"""
import os
from datetime import datetime, timedelta
from time import strftime

import requests
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def ingest_data(date):
    """Ingest ACLED data for a given date"""

    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    api_key = os.getenv("ACLED_ACCESS_KEY")
    username = os.getenv("ACLED_USERNAME")

    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    conf.set('spark.hadoop.fs.s3a.access.key', aws_access_key)
    conf.set('spark.hadoop.fs.s3a.secret.key', aws_secret_key)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    dataframe = spark.createDataFrame([], StructType([]))

    response = requests.get(f"""https://api.acleddata.com/acled/read?key={api_key}&email={username}&event_date={strftime("%Y-%m-%d", (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=10)).timetuple())}=&iso=804""", timeout=30)
    data = response.json()['data']
    dataframe2 = spark.read.json(spark.sparkContext.parallelize([data]))
    dataframe = dataframe.unionByName(dataframe2, True)
    if dataframe.count() == 0:
        return "No data"
    if dataframe.count() == 500:
        print("Max data limit reached. Calling API again.")
        response = requests.get(f"""https://api.acleddata.com/acled/read?key={api_key}&email={username}&event_date={strftime("%Y-%m-%d", (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=7)).timetuple())}=&iso=804&page=2""", timeout=30)
        data = response.json()['data']
        dataframe3 = spark.read.json(spark.sparkContext.parallelize([data]))
        dataframe = dataframe.unionByName(dataframe3, True)

    dataframe = dataframe.withColumn("event_date", F.to_date("event_date", "yyyy-MM-dd"))
    dataframe = dataframe.withColumn("fatalities", dataframe["fatalities"].cast("int"))
    dataframe = dataframe.withColumn("geo_precision", dataframe["geo_precision"].cast("int"))
    dataframe = dataframe.withColumn("inter1", dataframe["inter1"].cast("int"))
    dataframe = dataframe.withColumn("inter2", dataframe["inter2"].cast("int"))
    dataframe = dataframe.withColumn("interaction", dataframe["interaction"].cast("int"))
    dataframe = dataframe.withColumn("latitude", dataframe["latitude"].cast("double"))
    dataframe = dataframe.withColumn("longitude", dataframe["longitude"].cast("double"))
    dataframe = dataframe.withColumn("time_precision", dataframe["time_precision"].cast("int"))
    dataframe = dataframe.withColumnRenamed("timestamp", "upload_date")
    dataframe = dataframe.withColumn("upload_date", F.from_unixtime("upload_date", "yyyy-MM-dd HH:mm:ss"))
    dataframe = dataframe.withColumn("upload_date", F.to_timestamp("upload_date", "yyyy-MM-dd HH:mm:ss"))
    dataframe = dataframe.withColumn("year", F.year("year"))

    for column in dataframe.columns:
        dataframe = dataframe.withColumn(column, F.when(F.col(column) == "", None).otherwise(F.col(column)))

    dataframe.coalesce(1).write.partitionBy('event_date').mode("append").option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").parquet('s3a://acled-bucket/parquet/')
    return "Success"
