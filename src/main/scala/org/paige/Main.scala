package org.paige

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.eventhubs._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val connStr = ConnectionStringBuilder()
      .setNamespaceName(Conf.eventhubNamespace)
      .setEventHubName(Conf.eventhubName)
      .setSasKeyName(Conf.eventhubSasKeyName)
      .setSasKey(Conf.eventhubSasKey)
      .build

    val customEventhubParameters = EventHubsConf(connStr)
      .setStartingPosition(Conf.eventStartingPosition())
      .setMaxEventsPerTrigger(Conf.maxEventsPerTrigger)
      .setConsumerGroup(Conf.consumerGroup)

    val spark = SparkSession
      .builder
      .appName("Spark structured streaming")
      .getOrCreate

    val incomingStream = spark
      .readStream
      .format("eventhubs")
      .options(customEventhubParameters.toMap)
      .load

    val eh = new EventHandler(spark)
    val query = eh.processStream(incomingStream)
    query.awaitTermination
  }
}

class EventHandler(spark1: SparkSession) {
  val eventSchema = new StructType()
    .add("eventName", StringType)
    .add("eventAt", TimestampType)

  def process (df: DataFrame): DataFrame = {
    val spark2 = spark1
    import spark2.implicits._
    // Event Hub message format is JSON and contains "body" field
    // Body is binary, so we cast it to string to see the actual content of the message
    df
      .select(from_json($"body".cast(StringType), eventSchema) as "msg")
      .select($"msg.*")
      .groupBy(
        window($"eventAt", "10 seconds", "5 seconds"),
        $"eventName")
      .count
      .orderBy($"eventName", $"window")
  }

  def processStream (df: DataFrame) : StreamingQuery = {
    process(df)
      .writeStream
      .queryName("agg")
      .outputMode("complete")
      .format("console")
      .start
  }
}

