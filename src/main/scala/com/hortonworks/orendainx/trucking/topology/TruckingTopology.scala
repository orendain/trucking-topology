package com.hortonworks.orendainx.trucking.topology

import com.hortonworks.orendainx.trucking.topology.bolts.TruckEventAndTrafficDataMergeBolt
import com.typesafe.config.{ConfigFactory, Config => TypeConfig}
import com.typesafe.scalalogging.Logger
import org.apache.nifi.remote.client.SiteToSiteClient
import org.apache.nifi.storm.{NiFiBolt, NiFiSpout}
import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.{Config, StormSubmitter}

import scala.concurrent.duration._

/**
  * Companion object to [[TruckingTopology]] class.
  * Provides an entry point for passing configuration changes.
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
object TruckingTopology {

  // NiFi constants
  val NiFiUrl = "nifi.url"
  val NiFiOutputPortName = "nifi.output.port-name"
  val NiFiOutputBatchSize = "nifi.output.batch-size"

  val NiFiInputPortName = "nifi.input.port-name"
  val NiFiInputBatchSize = "nifi.input.batch-size"
  val NiFiInputTickFrequency = "nifi.input.tick-frequency"

  def main(args: Array[String]): Unit = {

    // Extract specified configuration file path from supplied argument, else throw Exception
    val configName = if (args.nonEmpty) args(0) else throw new IllegalArgumentException("Must specify name of configuration file.")
    val config = ConfigFactory.load(configName)

    // Set up configuration for the Storm Topology
    val stormConfig = new Config()
    stormConfig.setDebug(config.getBoolean(Config.TOPOLOGY_DEBUG))
    stormConfig.setMessageTimeoutSecs(config.getInt(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS))
    stormConfig.setNumWorkers(config.getInt(Config.TOPOLOGY_WORKERS))

    // TODO: would be nice if storm.Config had "setProperty" to hide hashmap implementation
    stormConfig.put("emptyConfig", new java.util.HashMap[String, String]) // Because storm -_-

    // Build out a TruckTopology
    val topology = new TruckingTopology(config).buildTopology()

    // Submit the topology to run on the cluster
    StormSubmitter.submitTopology("truckTopology", stormConfig, topology)
  }
}

/**
  * Create a topology with the following components.
  *
  * Spouts:
  *   - NiFiSpout (for injesting trucking events from NiFi)
  * Bolt:
  *   - RouterBolt
  *   - NiFiBolt (for sending back out to NiFi)
  *
  * @author Edgar Orendain <edgar@orendainx.com>
  */
class TruckingTopology(config: TypeConfig) {

  lazy val logger = Logger(classOf[TruckingTopology])

  /**
    *
    * @return a built StormTopology
    */
  def buildTopology(): StormTopology = {
    // Builder to perform the construction of the topology.
    implicit val builder = new TopologyBuilder()

    // Build Nifi Spouts to ingest trucking data
    buildNifiTruckEventSpout()
    buildNifiTrafficDataSpout()

    // Build Bolt to merge data streams together with windowing
    buildMergeBolt()

    // Build Nifi Bolt for pushing values back to Nifi
    buildNifiBolt()

    logger.info("Storm topology finished building.")

    // Finally, create the topology
    builder.createTopology()
  }

  def buildNifiTruckEventSpout()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val nifiUrl = config.getString(TruckingTopology.NiFiUrl)
    val nifiPortName = config.getString("truck-events.port-name")
    val batchSize = config.getInt("truck-events.batch-size")
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // This assumes that the data is text data, as it will map the byte array received from NiFi to a UTF-8 Encoded string.
    val client = new SiteToSiteClient.Builder()
      .url(nifiUrl)
      .portName(nifiPortName)
      .requestBatchCount(batchSize)
      .buildConfig()

    // Create a spout with the specified configuration, and place it in the topology blueprint
    val nifiSpout = new NiFiSpout(client)
    builder.setSpout("truckEvents", nifiSpout, taskCount)
  }

  def buildNifiTrafficDataSpout()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val nifiUrl = config.getString(TruckingTopology.NiFiUrl)
    val nifiPortName = config.getString("traffic-data.port-name")
    val batchSize = config.getInt("traffic-data.batch-size")
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // This assumes that the data is text data, as it will map the byte array received from NiFi to a UTF-8 Encoded string.
    val client = new SiteToSiteClient.Builder()
      .url(nifiUrl)
      .portName(nifiPortName)
      .requestBatchCount(batchSize)
      .buildConfig()

    // Create a spout with the specified configuration, and place it in the topology blueprint
    val nifiSpout = new NiFiSpout(client)
    builder.setSpout("trafficData", nifiSpout, taskCount)
  }

  def buildMergeBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)
    val duration = config.getInt(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)

    // Create a bolt with a tumbling window
    val windowDuration = new BaseWindowedBolt.Duration(duration, MILLISECONDS)
    val bolt = new TruckEventAndTrafficDataMergeBolt().withTumblingWindow(windowDuration)

    // Place the bolt in the topology blueprint
    builder.setBolt("mergeData", bolt, taskCount)
      .fieldsGrouping("truckEvents", new Fields("routeId"))
      .fieldsGrouping("trafficData", new Fields("routeId"))
  }

  def buildNifiBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val nifiUrl = config.getString(TruckingTopology.NiFiUrl)
    val nifiPortName = config.getString(TruckingTopology.NiFiInputPortName)
    val tickFrequency = config.getInt(TruckingTopology.NiFiInputTickFrequency)
    val batchSize = config.getInt(TruckingTopology.NiFiInputBatchSize)
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    val client = new SiteToSiteClient.Builder()
      .url(nifiUrl)
      .portName(nifiPortName)
      .buildConfig()

    val packetBuilder = new TruckingPacketBuilder()
    val nifiBolt = new NiFiBolt(client, packetBuilder, tickFrequency)
      .withBatchSize(batchSize)

    builder.setBolt("outToNifi", nifiBolt, taskCount)
      .shuffleGrouping("mergeData")
  }
}
