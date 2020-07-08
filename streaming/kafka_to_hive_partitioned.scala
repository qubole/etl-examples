/* In this example, it receive events from kafka source and dump it into a partitioned Hive table (on "dt" column).
Partition column is getting derived from one of the field from the source event. */

import java.sql.Timestamp

import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import spark.implicits._

val mySchema = (new StructType)
  .add("sno", StringType)
  .add("observation_date", StringType)
  .add("state", StringType)
  .add("country", StringType)
  .add("last_update", StringType)
  .add("confirmed", StringType)
  .add("deaths", StringType)
  .add("recovered", StringType)
  
/* Update your kafka end-point and checkpoint-location*/
val brokerEndPoint = "<your-end-point:port>"
val checkPointLocation = "<your-checkpoint-bukcet/cp1>"
/* Kafka topic and Hive sink detail*/
val tableName = "<db-name.table-name>"
val topic = "<your-kafka-topic-name>"

var ds_kafka = spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", brokerEndPoint)
.option("subscribe", topic)
.option("startingOffsets", "earliest")
.option("kafkaConsumer.pollTimeoutMs", 512)
.option("fetchOffset.numRetries", 3)
.option("fetchOffset.retryIntervalMs", 10)
.load()
val df_kafka = ds_kafka.selectExpr("CAST(value AS STRING)").as[(String)]

/* Deriving the partition column from the incoming event field last_update*/
val df1_kafka = df_kafka.select(from_json($"value", mySchema).as("data"))
.select("data.*")
.withColumn("dt", $"last_update".substr(0,10))

val query = df1_kafka.writeStream
.queryName("kafka_to_partitioned_hive_table")
.appendToTable(tableName)
.outputMode("Append")
.trigger(ProcessingTime("10 seconds"))
.option("table.metastore.stopOnFailure", "false")
.option("table.metastore.updateIntervalSeconds", 10)
.option("checkpointLocation", checkPointLocation)
.start()

query.awaitTermination
