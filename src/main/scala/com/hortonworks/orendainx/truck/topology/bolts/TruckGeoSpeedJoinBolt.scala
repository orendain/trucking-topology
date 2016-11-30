package com.hortonworks.orendainx.truck.topology

import java.util
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.windowing.TupleWindow
import scala.collection.JavaConversions._

class TruckGeoSpeedJoinBolt() extends BaseWindowedBolt {

  /*
   * Definition and implicit
   */
  class Values(val args: Any*)
  implicit def scalaValues2StormValues(v: Values): org.apache.storm.tuple.Values = {
    new org.apache.storm.tuple.Values(v.args.map(_.toString))
  }

  // TODO: wut.
  var collector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    // TODO: remove super calls? the call is a NOOP.  bring up in code review
    super.prepare(stormConf, context, collector)

    this.collector = collector
  }

  override def execute(inputWindow: TupleWindow): Unit = {

    // Extract all of the tuples from the TupleWindow
    val tuples = inputWindow.get().toList

    // Filter for tuples from specific streams, parse them into proper events and sort the resulting list by time of event
    val geoEvents = tuples.filter(_.getSourceComponent == "truckGeoEvents").map(TruckGeoEvent(_)).sortBy(_.eventTime.getTime)
    val speedEvents = tuples.filter(_.getSourceComponent == "truckSpeedEvents").map(TruckSpeedEvent(_)).sortBy(_.eventTime.getTime)

    // Zip corresponding geo/speed events together and combine them into a geospeed event before emitting that data downstream
    geoEvents.zip(speedEvents).map(z => TruckGeoSpeedEvent(z._1, z._2)).foreach { e =>
      collector.emit(new Values(e.eventTime, e.truckId, e.driverId, e.driverName, e.routeId, e.routeName, e.status, e.latitude, e.longitude, e.correlationId, e.eventKey, e.speed))
      if (e.status != "Normal")
        collector.emit("anomalies", new Values(e.eventTime, e.truckId, e.driverId, e.driverName, e.routeId, e.routeName, e.status, e.latitude, e.longitude, e.correlationId, e.eventKey, e.speed))
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    super.declareOutputFields(declarer)

    // Declare a stream with the default id
    declarer.declare(new Fields("eventTime", "truckId", "driverId", "driverName", "routeId", "routeName", "eventType", "latitude", "longitude", "correlationId", "eventKey", "speed"))

    // Declare a stream with a custom id
    declarer.declareStream("anomalousEvents", new Fields("eventTime", "truckId", "driverId", "driverName", "routeId", "routeName", "eventType", "latitude", "longitude", "correlationId", "eventKey", "speed"))
  }
}
