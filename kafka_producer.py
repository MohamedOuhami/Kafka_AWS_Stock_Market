# Importing the libraries
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json

# Create the Producer object
producer = KafkaProducer(
    bootstrap_servers=['13.51.175.255:9092'],value_serializer=lambda x : dumps(x).encode('utf-8')
)

# To test the producer, we can send some data to the consumer
producer.send('StockTopic',value="{'Mohamed':'Ouhami'}")

# Now, we're going to simulate a real-time data flow by going through each row and add It to the queue
stock_df = pd.read_csv('indexProcessed.csv')
stock_df.head()

# Send the data one by one to the consumer
while True:
    data = stock_df.sample(1).to_dict(orient="records")[0]
    producer.send('StockTopic',value=data)
    sleep(2)
