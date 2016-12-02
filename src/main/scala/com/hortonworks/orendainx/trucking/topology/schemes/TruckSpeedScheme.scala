package com.hortonworks.orendainx.trucking.topology.schemes

import java.nio.ByteBuffer
import java.sql.Timestamp

import com.typesafe.scalalogging.Logger
import org.apache.storm.tuple.{Fields, Values}

/**
  * Scheme for parsing speed events.
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
object TruckSpeedScheme extends DelimitedScheme("\\|") {

  lazy val logger = Logger(this.getClass)

  override def deserialize(buffer: ByteBuffer): Values = {

    logger.info("Deserializing string")

    val strings = deserializeString(buffer)

    // Extract data from buffer
    val eventTime = Timestamp.valueOf(strings(0))
    //val eventType = strings(1)
    val truckId = Integer.valueOf(strings(2))
    val driverId = Integer.valueOf(strings(3))
    val driverName = strings(4)
    val routeId = Integer.valueOf(strings(5))
    val routeName = strings(6)
    val speed = Integer.valueOf(strings(7))

    new Values(eventTime, truckId, driverId, driverName, routeId, routeName, speed)
  }

  override def getOutputFields: Fields =
    new Fields("eventTime", "truckId", "driverId", "driverName", "routeId", "routeName", "speed")
}
