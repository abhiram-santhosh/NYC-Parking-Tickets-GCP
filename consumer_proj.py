from pyspark import SparkContext, SQLContext
from itertools import chain
from pyspark.ml import PipelineModel, Pipeline
import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, decode, substring
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import from_json, udf, split
from kafka import KafkaConsumer
import json
import pandas as pd
#from sparknlp import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time

schema = StructType([
    StructField("ID", StringType()),
    StructField("Name", StringType()),
    StructField("Business", StringType()),
    StructField("Violation Precinct", IntegerType()),
    StructField("Feet From Curb", IntegerType()),
    StructField("Violation Time", StringType()),
    StructField("Violation In Front Of Or Opposite", StringType()),
    StructField("Issuer Precinct", IntegerType()),
    StructField("Street Code2", IntegerType()),
    StructField("From Hours In Effect", StringType()),
    StructField("Issuing Agency", StringType()),
    StructField("Street Code1", IntegerType()),
    StructField("Issuer Code", IntegerType()),
    StructField("Violation County", StringType()),
    StructField("Meter Number", StringType()),
    StructField("Plate Type", StringType()),
    StructField("Unregistered Vehicle?", StringType()),
    StructField("Issue Date", StringType()),
    StructField("Violation Post Code", StringType()),
    StructField("Street Code3", IntegerType()),
    StructField("Double Parking Violation", StringType()),
    StructField("Plate ID", StringType()),
    StructField("Violation Code", IntegerType()),
    StructField("Street Name", StringType()),
    StructField("Registration State", StringType()),
    StructField("Hydrant Violation", StringType()),
    StructField("Days Parking In Effect", StringType()),
    StructField("Vehicle Expiration Date", StringType()),
    StructField("Issuer Squad", StringType()),
    StructField("Vehicle Make", StringType()),
    StructField("Sub Division", StringType()),
    StructField("Intersecting Street", StringType()),
    StructField("Vehicle Year", IntegerType()),
    StructField("Vehicle Color", StringType()),
    StructField("Time First Observed", StringType()),
    StructField("Summons Number", LongType()),
    StructField("Law Section", IntegerType()),
    StructField("Violation Location", IntegerType()),
    StructField("To Hours In Effect", StringType()),
    StructField("Issuer Command", StringType()),
    StructField("Date First Observed", StringType()),
    StructField("House Number", StringType()),
    StructField("Violation Legal Code", StringType()),
    StructField("No Standing or Stopping Violation", StringType()),
    StructField("Violation Description", StringType()),
    StructField("Vehicle Body Type", StringType())
])

kafka_topic_name = "mm19b059"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == '__main__':
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("final_project") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of df: ")
    df.printSchema()

    model = PipelineModel.load('logistic_regression.model')
    print('Model Pipeline Loaded')

    print('Spark session active.')
    consumer = KafkaConsumer(kafka_topic_name, bootstrap_servers = [kafka_bootstrap_servers])
    for msg in consumer:
        print(msg)
        message = json.loads(msg.value)
        df_line = pd.json_normalize(message)
        df = df_line.copy()
        df_line = spark.createDataFrame(df_line, schema =schema)
        df = spark.createDataFrame(df, schema = schema)

    print(df)

    prediction = model.transform(df)
    prediction = prediction.select(['target', 'prediction'])
    
    f1_eval = MulticlassClassificationEvaluator(labelCol="target", predictionCol="prediction", metricName='f1')
    evaluator = MulticlassClassificationEvaluator(labelCol="target", predictionCol="prediction", metricName="accuracy")
    acc = evaluator.evaluate(prediction) * 100
    f1 = f1_eval.evaluate(prediction)

    print("Accuracy and F1-score for batch {}: ".format(batch), acc, f1)
    print("------------------------")