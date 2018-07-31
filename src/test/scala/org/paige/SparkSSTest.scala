package org.paige

import org.apache.spark.sql.SparkSession
import org.scalatest._

class ConfigTest extends FlatSpec with Matchers {
  "config file" should "include eventhubNamespace" in {
    Conf.eventhubNamespace should not be empty
  }
}

class StructuredStreamingTest extends FlatSpec with Matchers with SparkSessionSetup {
  "open,close,open,close,open at :00,:01,:10,:11,:12 " should
    "result in open 1,1,2,2 and close 1,1,1,1 given 10 sec agg window sliding every 5 sec" in withSparkSession {
    (sparkSession) =>
      val spark2 = sparkSession
      import spark2.implicits._
      val event1 = "{\"eventName\":\"open\",\"eventAt\":\"2018-07-11 10:00:00\"}".getBytes()
      val event2 = "{\"eventName\":\"close\",\"eventAt\":\"2018-07-11 10:00:01\"}".getBytes()
      val event3 = "{\"eventName\":\"open\",\"eventAt\":\"2018-07-11 10:00:10\"}".getBytes()
      val event4 = "{\"eventName\":\"close\",\"eventAt\":\"2018-07-11 10:00:11\"}".getBytes()
      val event5 = "{\"eventName\":\"open\",\"eventAt\":\"2018-07-11 10:00:12\"}".getBytes()
      val df = List(event1, event2, event3, event4, event5).toDF("body")

      val eh = new EventHandler(sparkSession)
      val result = eh.process(df)

      //check results of open events
      val opendf = result.filter($"eventName" === "open")
      opendf.count should be(4)
      opendf.filter(
        $"window.start" === "2018-07-11T09:59:55" &&
          $"window.end" === "2018-07-11T10:00:05")
        .first.getLong(2) should be (1)
      opendf.filter(
        $"window.start" === "2018-07-11T10:00:00" &&
          $"window.end" === "2018-07-11T10:00:10")
        .first.getLong(2) should be (1)
      opendf.filter(
        $"window.start" === "2018-07-11T10:00:05" &&
          $"window.end" === "2018-07-11T10:00:15")
        .first.getLong(2) should be (2)
      opendf.filter(
        $"window.start" === "2018-07-11T10:00:10" &&
          $"window.end" === "2018-07-11T10:00:20")
        .first.getLong(2) should be (2)

      //check results of close events
      val closedf = result.filter($"eventName" === "close")
      closedf.count should be(4)
      closedf.filter(
        $"window.start" === "2018-07-11T09:59:55" &&
          $"window.end" === "2018-07-11T10:00:05")
        .first.getLong(2) should be (1)
      closedf.filter(
        $"window.start" === "2018-07-11T10:00:00" &&
          $"window.end" === "2018-07-11T10:00:10")
        .first.getLong(2) should be (1)
      closedf.filter(
        $"window.start" === "2018-07-11T10:00:05" &&
          $"window.end" === "2018-07-11T10:00:15")
        .first.getLong(2) should be (1)
      closedf.filter(
        $"window.start" === "2018-07-11T10:00:10" &&
          $"window.end" === "2018-07-11T10:00:20")
        .first.getLong(2) should be (1)
  }
}

trait SparkSessionSetup {
  def withSparkSession(testMethod: (SparkSession) => Any) {
    val hostname = sys.env("HOSTNAME")
    val spark = SparkSession
      .builder
      .master("spark://" + hostname + ":7077")
      .appName("spark structured streaming test")
      .getOrCreate
    try {
      testMethod(spark)
    }
    finally spark.stop
  }
}
