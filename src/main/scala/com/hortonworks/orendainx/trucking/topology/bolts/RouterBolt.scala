package com.hortonworks.orendainx.trucking.topology.bolts

import java.nio.charset.StandardCharsets
import java.util

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

  lazy val log = Logger(this.getClass)

  var outputCollector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    outputCollector = collector
  }

  override def execute(inputWindow: TupleWindow): Unit = {
    log.info("Executing")

    // Extract all of the tuples from the TupleWindow and parse them into proper event classes
    //val geoEvents = inputWindow.get().map(TruckingEvent(_))

    inputWindow.get().foreach { tuple =>
      log.info(s"Raw: ${tuple.toString}")
      val dp = tuple.getValueByField("nifiDataPacket").asInstanceOf[NiFiDataPacket]
      log.info(s"Attributs: ${dp.getAttributes}")
      log.info(s"Content: ${dp.getContent}")
      val contentStr = new String(dp.getContent, StandardCharsets.UTF_8)
      log.info(s"Content2: $contentStr")
      outputCollector.emit(new Values(contentStr))
    }

    /*
    2016-12-21 20:07:54.049 c.h.o.t.t.b.RouterBolt [INFO] Raw: source: truckingEvents:5, stream: default, id: {}, [org.apache.nifi.storm.StandardNiFiDataPacket@150e79e6]
2016-12-21 20:07:54.049 c.h.o.t.t.b.RouterBolt [INFO] Attributs: {path=./, mime.type=text/plain, filename=data.0-21546.txt, telemetry_device_id=1, uuid=b4dc46a5-702c-4c0f-aba5-db58309c0bf5}
2016-12-21 20:07:54.049 c.h.o.t.t.b.RouterBolt [INFO] Content: [B@2711f3ad
     */

    // Emit data downstream, but give special consideration to anomalous events
//    geoEvents.foreach { event =>
//      outputCollector.emit(event.toStormValues)
//      if (event.eventType != "Normal")
//        outputCollector.emit("anomalousEvents", event.toStormValues)
//    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("someField"))
    //declarer.declare(TruckingEventScheme.getOutputFields)
  }
}
