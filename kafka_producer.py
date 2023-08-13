from kafka import KafkaProducer
import sys
import time
from json import dumps
from google.cloud import storage
from pyspark.sql import SparkSession

KAFKA_TOPIC_NAME_CONS = "mm19b059"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started")
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS)
    
    datset_loc = sys.argv[1]
    batch_size = sys.argv[2]
    
    print(f"Reading Dataset from {datset_loc} with a batch size of {batch_size}")
    
    spark = SparkSession.builder \
                .appName("final_project") \
                    .getOrCreate()
    df = spark.read.format("csv").option("header", "true").load(datset_loc)

    message_list = []
    message = []
    
    batch, count = [], 0
    
    data_itr = df.rdd.toLocalIterator()
    for row in data_itr:       
        batch.append(row)
        count += 1
        
        if len(batch) == batch_size:
            for row in batch:
                
                producer.send(KAFKA_TOPIC_NAME_CONS, value=str(row).encode())
            print('sent batch')
            time.sleep(10)
            batch = []
        
    
    print("Kafka Producer Application Completed. ")
    producer.flush()
    producer.close()