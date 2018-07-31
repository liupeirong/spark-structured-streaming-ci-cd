package org.paige

import com.typesafe.config._
import org.apache.spark.eventhubs.EventPosition

object Conf {
  private val _appconf = ConfigFactory.load("app")
  val eventhubNamespace = _appconf.getString("sparkss.eventhubNamespace")
  val eventhubName = _appconf.getString("sparkss.eventhubName")
  val eventhubSasKeyName = _appconf.getString("sparkss.eventhubSasKeyName")
  val eventhubSasKey = _appconf.getString("sparkss.eventhubSasKey")
  val eventStartingPosition: () => EventPosition = () => {
    val pos = _appconf.getString("sparkss.eventStartingPosition")
    if (pos.equalsIgnoreCase("earliest"))
      EventPosition.fromStartOfStream
    else if (pos.equalsIgnoreCase("latest"))
      EventPosition.fromEndOfStream
    else
      throw new Exception("eventStartingPosition value not supported")
  }
  val maxEventsPerTrigger = _appconf.getInt("sparkss.maxEventsPerTrigger")
  val consumerGroup = _appconf.getString("sparkss.consumerGroup")
}
