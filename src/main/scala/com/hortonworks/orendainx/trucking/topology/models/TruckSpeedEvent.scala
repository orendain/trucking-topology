package com.hortonworks.orendainx.trucking.topology.models

import java.sql.Timestamp

import org.apache.storm.tuple.Tuple

/**
  * Model for speed events.
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
case class TruckSpeedEvent(eventTime: Timestamp, truckId: Int, driverId: Int, driverName: String,
                           routeId: Int, routeName: String, speed: Int)

case object TruckSpeedEvent {

  def apply(tuple: Tuple): TruckSpeedEvent =
    TruckSpeedEvent(
      tuple.getValueByField("eventTime").asInstanceOf[Timestamp],
      tuple.getIntegerByField("truckId"),
      tuple.getIntegerByField("driverId"),
      tuple.getStringByField("driverName"),
      tuple.getIntegerByField("routeId"),
      tuple.getStringByField("routeName"),
      tuple.getIntegerByField("speed"))
}
