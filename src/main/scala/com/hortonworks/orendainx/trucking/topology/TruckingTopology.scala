package com.hortonworks.orendainx.trucking.topology

import java.util.Properties

import com.hortonworks.orendainx.trucking.shared.schemes.TruckingEventScheme
import com.hortonworks.orendainx.trucking.topology.bolts.RouterBolt
import com.typesafe.config.{ConfigFactory, Config => TypeConfig}
import com.typesafe.scalalogging.Logger
import org.apache.nifi.remote.client.{SiteToSiteClient, SiteToSiteClientConfig}
import org.apache.nifi.storm.{NiFiBolt, NiFiDataPacketBuilder, NiFiSpout}
import org.apache.storm.generated.StormTopology
import org.apache.storm.hbase.bolt.HBaseBolt
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector
import org.apache.storm.kafka.{KafkaSpout, SpoutConfig, ZkHosts}
import org.apache.storm.spout.SchemeAsMultiScheme
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
  val NiFiInputTickFrequency = "nifi.output.tick-frequency"

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
    stormConfig.put("emptyConfig", new java.util.HashMap[String, String]) // Because current version of storm is the way it is -_-

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
  *   - KafkaSpout (for geo events)
  *   - KafkaSpout (for speed events)
  * Bolt:
  *   - TruckGeoSpeedJoinBolt
  *   - HBaseBolt (all events)
  *   - HBaseBolt (anomalous events)
  *   - HBaseBolt (anomalous event count)
  *   - KafkaBolt (outbound)
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

    // Build Nifi Spout to ingest trucking events
    buildNifiSpout()

    // Built Bolt to route data to multiple streams
    buildRouterBolt()

    // Build Nifi Bolt for pushing values back to Nifi
    buildNifiBolt()

    logger.info("Storm topology finished building.")

    // Finally, create the topology
    builder.createTopology()
  }

  def buildNifiSpout()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val nifiUrl = config.getString(TruckingTopology.NiFiUrl)
    val nifiPortName = config.getString(TruckingTopology.NiFiOutputPortName)
    //val batchSize = config.getString(TruckingTopology.NiFiOutputBatchSize)
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // This example assumes that NiFi exposes an OutputPort on the root group named "Data For Storm".
    // Additionally, it assumes that the data that it will receive from this OutputPort is text data, as it will map the byte array received from NiFi to a UTF-8 Encoded string.
    val client = new SiteToSiteClient.Builder()
      .url(nifiUrl)
      .portName(nifiPortName)
      //.requestBatchCount(batchSize)
      .buildConfig()

    // Create a spout with the specified configuration, and place it in the topology blueprint
    val nifiSpout = new NiFiSpout(client)
    builder.setSpout("truckingEvents", nifiSpout, taskCount)
  }

  def buildRouterBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)
    val duration = config.getInt(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)

    // Create a bolt with a tumbling window
    val windowDuration = new BaseWindowedBolt.Duration(duration, MILLISECONDS)
    val bolt = new RouterBolt().withTumblingWindow(windowDuration)

    // Place the bolt in the topology blueprint
    builder.setBolt("joinTruckEvents", bolt, taskCount)
      .shuffleGrouping("truckingEvents")
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

    val packetBuilder = new TruckingDataBuilder()
    val nifiBolt = new NiFiBolt(client, packetBuilder, tickFrequency)
      //.withBatchSize(batchSize)

    builder.setBolt("pushOutTruckingEvents", nifiBolt, taskCount)
      .shuffleGrouping("joinTruckEvents")
  }
}
