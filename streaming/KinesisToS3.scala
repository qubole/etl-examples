/* In this example, it receive events from Kinesis Stream and dump it into a given s3 location.
Note: The data is partitioned by year/month/day/hour which is derived from the arrival time */

import java.nio.ByteBuffer
import java.sql.Timestamp
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.auth._
import scala.concurrent.duration._
import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import spark.implicits._


val kinesisStreamName = "kinesisStreamName"
val kinesisRegion = "aws-region" // e.g. us-west-2 , ap-southeast-1
val endpointUrl = "kinesis-end-point" // e.g. https://kinesis.ap-southeast-1.amazonaws.com

val parquetOutputPath = "s3://output/path/data/" // S3 path 
val checkPointPath = "s3://checkpoint/localation/" // Local or S3 path


val mySchema = (new StructType)
  .add("dt", StringType)
  .add("col1", StringType)
  .add("col2", StringType)
  .add("col3", StringType)
  .add("col4", StringType)
  .add("timestamp", StringType)

var ds_kinesis = spark
.readStream
.format("kinesis")
.option("streamName", kinesisStreamName)
.option("endpointUrl", endpointUrl)
.option("regionName", kinesisRegion)
.option("startingPosition", "earliest")
.option("kinesis.executor.maxFetchTimeInMs", 10000)
.option("kinesis.executor.maxFetchRecordsPerShard", 500000)
.option("kinesis.executor.maxRecordPerRead", 10000)
.load()

val df_kinesis = ds_kinesis.selectExpr("CAST(data AS STRING)", "CAST(approximateArrivalTimestamp AS TIMESTAMP)").as[(String, Timestamp)]
val df1_kinesis = df_kinesis.select(from_json($"data", mySchema).as("data2"), $"approximateArrivalTimestamp").select("data2.*", "approximateArrivalTimestamp").withColumn("date", $"approximateArrivalTimestamp".cast("timestamp")).withColumn("year", year(col("date")))
.withColumn("month", month(col("date")))
.withColumn("day", dayofmonth(col("date")))
.withColumn("hour", hour(col("date")))

val query = df1_kinesis.writeStream
.outputMode("Append")
.trigger(ProcessingTime("60 seconds"))
.format("parquet")
.partitionBy("year", "month", "day", "hour")
.option("path", parquetOutputPath)
.option("checkpointLocation", checkPointPath)
.start()



query.awaitTermination
