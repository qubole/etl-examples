# This is the class to send the notification to a slack web-hook
class Slack_ForeachWriter:
  '''
  Class to send alerts to a Slack Channel.
  When used with `foreach`, copies of this class is going to be used to write
  multiple rows in the executor. See the python docs for `DataStreamWriter.foreach`
  for more details.
  '''
  
  def __init__(self, url):
    self.webhook_url = url

  def open(self, partition_id, epoch_id):
    # This is called first when preparing to send multiple rows.
    # Put all the initialization code inside open() so that a fresh
    # copy of this class is initialized in the executor where open()
    # will be called. 
    # This will be a no-op for our case
    return True

  def process(self, row):
      
    # This is called for each row after open() has been called.
    # This implementation sends one row at a time. 
    # Column names in row depend on the column names in streaming dataframe.
    # In this case, row has columns `value` and `count`. Modify coulmn names depending on your schema.
    # We check a predicate and send alert to slack channel if predicate is true.
    
    import json
    import requests
    
    print(row['count'])
    print(self.webhook_url)
    
    # Modify Predicate Here, Right now it checks if count of a row is multiple of 5
    if row['count'] % 5 == 0:                        
        
        slack_data = {'text': "Reached {} count for {}".format(row['count'], row['value'])}              
    
        response = requests.post(
            self.webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            raise ValueError(
                'Request to slack returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
            )

  def close(self, err):
    # This is called after all the rows have been processed.
    if err:
      raise err
	  
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("stream_analytics_notify_ashish_app").getOrCreate()

# Modify webhook_url and Kafka end-point here
webhook_url = 'https://hooks.slack.com/services/ABCDEFGHIJKL'
brokerEndPoint = "<your-end-point:port>"
# Modify topic-name here 
topic = "<your-kafka-topic-name>"
# Modify checkpoint-location here 
checkpoint_location = "<your-checkpoint-bukcet/cp1>"                                                

query = (
  spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", brokerEndPoint)  
  .option("subscribe", "qubole")                                                        
  .option("startingOffsets", "earliest")
  .load()
    # Kafka schema has key, value, we select value and do an aggregated count
    # Modify the logic based on your schema
    .selectExpr("CAST(value as STRING)")
    .groupBy("value")
    .count()
    .toDF("value", "count")
    .writeStream
    .queryName("stream_analytics_notify_ashish")
    .option("checkpointLocation", checkpoint_location)                           
    .foreach(Slack_ForeachWriter(webhook_url))
    .outputMode("complete")
    .start()
)

query.awaitTermination()
