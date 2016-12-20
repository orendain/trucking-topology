package com.hortonworks.orendainx.trucking.topology.bolts

import java.util

import com.hortonworks.orendainx.trucking.shared.models.TruckingEvent
import com.hortonworks.orendainx.trucking.shared.schemes.TruckingEventScheme
import com.typesafe.scalalogging.Logger
import org.apache.nifi.storm.NiFiDataPacket
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.windowing.TupleWindow

import scala.collection.JavaConversions._

/**
  * Bolt responsible for routing data to multiple streams.
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
class RouterBolt() extends BaseWindowedBolt {

  lazy val logger = Logger(this.getClass)

  var outputCollector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    outputCollector = collector
    logger.info("Preparations finished")
  }

  override def execute(inputWindow: TupleWindow): Unit = {
    logger.info("Executing")

    // Extract all of the tuples from the TupleWindow and parse them into proper event classes
    //val geoEvents = inputWindow.get().map(TruckingEvent(_))

    inputWindow.get().foreach { tuple =>
      logger.info(s"Raw: ${tuple.toString}")
      val dp = tuple.getValueByField("nifiDataPacket").asInstanceOf[NiFiDataPacket]
      logger.info(s"Attributs: ${dp.getAttributes}")
      logger.info(s"Content: ${dp.getContent.toString}")
      outputCollector.emit(new Values(dp.getContent.toString))
    }

    // Emit data downstream, but give special consideration to anomalous events
//    geoEvents.foreach { event =>
//      outputCollector.emit(event.toStormValues)
//      if (event.eventType != "Normal")
//        outputCollector.emit("anomalousEvents", event.toStormValues)
//    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    // Declare a stream with the default id and one with a custom id
    declarer.declare(new Fields("someField"))
    //declarer.declare(TruckingEventScheme.getOutputFields)
    //declarer.declareStream("anomalousEvents", TruckingEventScheme.getOutputFields)
  }
}
