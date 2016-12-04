package com.hortonworks.orendainx.trucking.topology.bolts

import java.util

import com.hortonworks.orendainx.trucking.shared.models.TruckingEvent
import com.hortonworks.orendainx.trucking.shared.schemes.TruckingEventScheme
import com.typesafe.scalalogging.Logger
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.windowing.TupleWindow

import scala.collection.JavaConversions._

/**
  * Bolt responsible for routing data to multiple streams.
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
class RouterBolt() extends BaseWindowedBolt {

  lazy val logger = Logger(this.getClass)

  /*
   * Definition and implicit of type 'Values' that acts as a bridge between Scala and Storm's Java [[Values]]
   */
  class Values(val args: Any*)
  implicit def scalaValues2StormValues(v: Values): org.apache.storm.tuple.Values = {
    new org.apache.storm.tuple.Values(v.args.map(_.toString))
  }

  var outputCollector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    outputCollector = collector
    logger.info("Preparations finished")
  }

  override def execute(inputWindow: TupleWindow): Unit = {
    logger.info("Executing")

    // Extract all of the tuples from the TupleWindow and parse them into proper event classes
    val geoEvents = inputWindow.get().map(TruckingEvent(_))

    // Emit data downstream, but give special consideration to anomalous events
    geoEvents.foreach { event =>
      outputCollector.emit(event.toStormValues)
      if (event.eventType != "Normal")
        outputCollector.emit("anomalousEvents", event.toStormValues)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    // Declare a stream with the default id and one with a custom id
    declarer.declare(TruckingEventScheme.getOutputFields)
    declarer.declareStream("anomalousEvents", TruckingEventScheme.getOutputFields)
  }
}
