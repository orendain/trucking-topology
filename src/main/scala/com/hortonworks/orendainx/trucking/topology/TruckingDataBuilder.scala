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
class TruckingDataBuilder extends NiFiDataPacketBuilder with Serializable {

  lazy val logger = Logger(this.getClass)

  override def createNiFiDataPacket(tuple: Tuple) = {
    logger.info(s"DBRaw: ${tuple.toString}")
    val value = tuple.getValue(0)
    logger.info(s"Value: ${value}")

    val sb = StringBuilder.newBuilder + value.toString

    val attrs = new util.HashMap[String, String]()
    attrs.put("testAttr1", "1")
    attrs.put("testAttr2", "2")

    new StandardNiFiDataPacket(sb.getBytes(StandardCharsets.UTF_8), attrs)
  }
}
