package com.hortonworks.orendainx.truck.topology

import java.sql.Timestamp

import org.apache.storm.tuple.Tuple

/**
  * Created by Edgar Orendain on 11/19/16.
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
