# Importing the libraries
import pandas as pd
from kafka import KafkaConsumer
from time import sleep
from json import loads
import json
import os
# Creating the kafka_consumer
consumer = KafkaConsumer(
    'StockTopic',bootstrap_servers=['13.51.175.255:9092'],value_deserializer=lambda x : loads(x.decode('utf-8'))
)

# Now, we test the consumer by printing the values in the queue
for c in consumer : 
    print(c.value)
    
# Our goal is to process the data and then store It in our S3 bucket for each entry
from s3fs import S3FileSystem

s3 = S3FileSystem()

for count,i in enumerate(consumer):
    print(i.value)
    
    # The S3 Bucket URL is stored in an environemt variables for security purposes
    with s3.open(os.getenv("S3_BUCKET_URL")+"/stock_market{}.json".format(count),'w') as file:
        json.dump(i.value,file)
        print("Uploaded file")
        
        