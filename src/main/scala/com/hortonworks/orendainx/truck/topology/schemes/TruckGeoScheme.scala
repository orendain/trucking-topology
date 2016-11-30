package com.hortonworks.orendainx.truck.topology

import java.nio.ByteBuffer
import org.apache.storm.tuple.{Fields, Values}
import java.sql.Timestamp

// TODO: consider re-structuring events ... would a geo device actually be signaling a speed violation?
// TODO: and would a speed monitoring device know about the current druver and route? (meh, maybe - but prlly just truckID)
/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */
object TruckGeoScheme extends DelimitedScheme("\\|") {

    // TODO: try removing override
  override def deserialize(buffer: ByteBuffer): Values = {

    implicit def int2Integer(x: Int): Object = java.lang.Integer.valueOf(x)

    val strings = deserializeString(buffer)

    // Extract data from buffer
    val eventTime = Timestamp.valueOf(strings(0))
    //val eventType = strings(1)
    val truckId = strings(2)
    val driverId = strings(3)
    val driverName = strings(4)
    val routeId = strings(5)
    val routeName = strings(6)
    val status = strings(7) // TODO: was renamed from "eventType" ... find better title?
    val latitude = strings(8)
    val longitude = strings(9)
    val correlationId = strings(10)
    val eventKey = s"$driverId|$truckId|${Long.MaxValue-eventTime.getTime}"

    new Values(eventTime, truckId, driverId, driverName, routeId, routeName, status, latitude, longitude, correlationId, eventKey)
  }

  override def getOutputFields: Fields =
    new Fields("eventTime", "truckId", "driverId", "driverName", "routeId", "routeName", "status", "latitude", "longitude", "correlationId", "eventKey")
}
