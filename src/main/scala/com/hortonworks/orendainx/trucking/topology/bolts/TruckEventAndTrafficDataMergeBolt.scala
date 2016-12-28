package com.hortonworks.orendainx.trucking.topology.bolts

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util

import com.hortonworks.orendainx.trucking.shared.models.{TrafficData, TruckEvent}
import com.hortonworks.registries.schemaregistry.{SchemaCompatibility, SchemaMetadata}
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.hortonworks.registries.schemaregistry.serdes.avro.{AvroSnapshotDeserializer, AvroSnapshotSerializer}
import com.typesafe.scalalogging.Logger
import org.apache.nifi.storm.NiFiDataPacket
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.windowing.TupleWindow

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Bolt responsible for routing data to multiple streams.
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
class TruckEventAndTrafficDataMergeBolt() extends BaseWindowedBolt {

  lazy val log = Logger(this.getClass)

  var outputCollector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    outputCollector = collector
  }

  override def execute(inputWindow: TupleWindow): Unit = {
    log.info("Executing")

    // Extract all of the tuples from the TupleWindow and parse them into proper event classes
    //val geoEvents = inputWindow.get().map(TruckingEvent(_))

    val truckEvents = mutable.HashMap.empty[Int, ListBuffer[TruckEvent]]
    val trafficData = mutable.HashMap.empty[Int, ListBuffer[TrafficData]]

    inputWindow.get().foreach { tuple =>
      log.info(s"Raw: ${tuple.toString}")
      val dp = tuple.getValueByField("nifiDataPacket").asInstanceOf[NiFiDataPacket]
      log.info(s"Attributs: ${dp.getAttributes}")
      log.info(s"Content: ${dp.getContent}")
      val contentStr = new String(dp.getContent, StandardCharsets.UTF_8)
      log.info(s"Content2: $contentStr")

      val fields = contentStr.split("\\|")



      val clientConfig = new java.util.HashMap[String, AnyRef]()
      //val schemaRegistryUrl = baseConfig.getString("schema-registry.url")
      val schemaRegistryUrl = "http://sandbox.hortonworks.com:9090/nifi"
      clientConfig.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl)
      val schemaRegistryClient = new SchemaRegistryClient(clientConfig)


      val schemaMetadata = new SchemaMetadata.Builder("TruckEvent")
        .`type`(AvroSchemaProvider.TYPE)
        .schemaGroup("trucking")
        .description("Sample schema")
        .compatibility(SchemaCompatibility.BACKWARD)
        .build()

      val avroType = AvroSchemaProvider.TYPE
      val serializer = schemaRegistryClient.getDefaultSerializer(avroType).asInstanceOf[AvroSnapshotSerializer]
      serializer.init(clientConfig) // TODO: bug could be that this is not implemented ... or should it even be for default avro?

      val deserializer = schemaRegistryClient.getDefaultDeserializer(avroType).asInstanceOf[AvroSnapshotDeserializer]
      val desObj = deserializer.deserialize(new ByteArrayInputStream(dp.getContent), schemaMetadata, null).asInstanceOf[TruckEvent]

      outputCollector.emit(new Values(desObj.toCSV))


      /*
      if (fields.size > 3) { // TruckEvent
        val data = TruckEvent(Timestamp.valueOf(fields(0)), fields(1).toInt, fields(2).toInt, fields(3), fields(4).toInt,
          fields(5), fields(6), fields(7), fields(8).toInt, fields(9))

        val buffer = truckEvents.getOrElse(data.routeId, ListBuffer.empty[TruckEvent])
        buffer += data
        truckEvents += (data.routeId -> buffer)

      } else { // TrafficData
        val data = TrafficData(Timestamp.valueOf(fields(0)), fields(1).toInt, fields(2).toInt)
        val buffer = trafficData.getOrElse(data.routeId, ListBuffer.empty[TrafficData])
        buffer += data
        trafficData += (data.routeId -> buffer)
      }*/
    }

    /*truckEvents.flatMap { case (routeId, lst) =>
      lst.map { event =>
        val trafficLst = trafficData(routeId)
        //trafficLst.map(data => data.eventTime.getTime - event.eventTime.getTime)
        trafficLst.sortBy(data => data.eventTime.getTime - event.eventTime.getTime)

        // val mergedData = new TruckAndTrafficData(event.timestamp, event.driverid, ...)
        //outputCollector.emit(new Values(mergedData.toCSV))
      }
    }*/


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
    declarer.declare(new Fields("CSVFormatObj"))
    //declarer.declare(TruckingEventScheme.getOutputFields)
  }
}
