package com.hortonworks.orendainx.truck.topology.models

import java.sql.Timestamp

import org.apache.storm.tuple.Tuple

/**
  * Model for the data that is resulted between a join of geo and speed events.
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
case class TruckGeoSpeedEvent(eventTime: Timestamp, truckId: Int, driverId: Int, driverName: String,
                         routeId: Int, routeName: String, status: String, latitude: Double, longitude: Double,
                         correlationId: Long, eventKey: String, speed: Int)

object TruckGeoSpeedEvent {

  def apply(geoEvent: TruckGeoEvent, speedEvent: TruckSpeedEvent): TruckGeoSpeedEvent =
    TruckGeoSpeedEvent(
      geoEvent.eventTime,
      geoEvent.truckId,
      geoEvent.driverId,
      geoEvent.driverName,
      geoEvent.routeId,
      geoEvent.routeName,
      geoEvent.status,
      geoEvent.latitude,
      geoEvent.longitude,
      geoEvent.correlationId,
      geoEvent.eventKey,
      speedEvent.speed)

  def apply(tuple: Tuple): TruckGeoSpeedEvent =
    TruckGeoSpeedEvent(
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
      tuple.getStringByField("eventKey"),
      tuple.getIntegerByField("speed"))
}
