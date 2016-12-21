package com.hortonworks.orendainx.trucking.topology

import java.nio.charset.StandardCharsets
import java.util

import com.typesafe.scalalogging.Logger
import org.apache.nifi.storm.{NiFiDataPacketBuilder, StandardNiFiDataPacket}
import org.apache.storm.tuple.Tuple

/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */

//Exception in thread "main" java.lang.IllegalStateException: Bolt 'pushOutTruckingEvents' contains a non-serializable field of type com.hortonworks.orendainx.trucking.topology.TruckingDataBuilder, which was instantiated prior to topology creation. com.hortonworks.orendainx.trucking.topology.TruckingDataBuilder should be instantiated within the prepare method of 'pushOutTruckingEvents at the earliest.
class TruckingPacketBuilder extends NiFiDataPacketBuilder with Serializable {

  lazy val logger = Logger(this.getClass)

  override def createNiFiDataPacket(tuple: Tuple) = {
    logger.info(s"DBRaw: ${tuple.toString}")
    val value = tuple.getValue(0)
    logger.info(s"Value: ${value}")

    /*
    2016-12-21 20:07:58.127 c.h.o.t.t.TruckingDataBuilder [INFO] DBRaw: source: joinTruckEvents:3, stream: default, id: {}, [[B@2711f3ad]
2016-12-21 20:07:58.127 c.h.o.t.t.TruckingDataBuilder [INFO] Value: [B@2711f3ad
     */

    val sb = StringBuilder.newBuilder + value.toString

    val attrs = new util.HashMap[String, String]()
    attrs.put("testAttr1", "1")
    attrs.put("testAttr2", "2")

    new StandardNiFiDataPacket(sb.getBytes(StandardCharsets.UTF_8), attrs)
  }
}
