package com.hortonworks.orendainx.truck.topology.models

import java.sql.Timestamp

import org.apache.storm.tuple.Tuple

/**
  * Model for geo events.
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
case class TruckGeoEvent(eventTime: Timestamp, truckId: Int, driverId: Int, driverName: String,
                         routeId: Int, routeName: String, status: String, latitude: Double, longitude: Double,
                         correlationId: Long, eventKey: String)

case object TruckGeoEvent {

  def apply(tuple: Tuple): TruckGeoEvent =
    TruckGeoEvent(
      tuple.getValueByField("eventTime").asInstanceOf[Timestamp],
      tuple.getIntegerByField("truckId"),
      tuple.getIntegerByField("driverId"),
      tuple.getStringByField("driverName"),
      tuple.getIntegerByField("routeId"),
      tuple.getStringByField("routeName"),
      tuple.getStringByField("status"),
      tuple.getDoubleByField("latitude"),
      tuple.getDoubleByField("longitude"),
      tuple.getLongByField("correlationId"),
      tuple.getStringByField("eventKey")
    )
}
