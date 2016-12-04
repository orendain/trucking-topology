package com.hortonworks.orendainx.trucking.topology

import java.util.Properties

import com.hortonworks.orendainx.trucking.shared.schemes.TruckingEventScheme
import com.hortonworks.orendainx.trucking.topology.bolts.RouterBolt
import com.hortonworks.orendainx.trucking.topology.schemes.TruckingEventScheme
import com.typesafe.config.{ConfigFactory, Config => TypeConfig}
import com.typesafe.scalalogging.Logger
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

  // Kafka consumer group id
  val ConsumerGroupId = "kafka.consumer.group.id"

  // Kafka topic constants
  val TruckGeoTopic = "kafka.consumer.truck-geo.topic"
  val TruckSpeedTopic = "kafka.consumer.truck-geo.topic"
  val TruckingTopic = "kafka.consumer.trucking.topic"

  // Kafka producer configuration constants
  val KafkaBootstrapServers = "kafka.producer.bootstrap-servers"
  val KafkaKeySerializer = "kafka.producer.key-serializer"
  val KafkaValueSerializer = "kafka.producer.value-serializer"

  // HBase constants
  val EventKeyField = "hbase.event-key-field" // TODO: shared with model classes, abstract out
  val ColumnFamily = "hbase.column-family"
  val AllTruckingEvents = "hbase.all-trucking.table"
  val AnomalousTruckingEvents = "hbase.anomalous-trucking.table"
  val AnomalousTruckingEventsCount = "hbase.anomalous-trucking-count.table"


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

    // Build Kafka Spouts to ingest trucking events
    buildKafkaSpout()

    // Built Bolt to route data to multiple streams
    buildRouterBolt()

    // Build HBase Bolts to persist all events, as well as specific anomalies
    buildAllTruckingEventsHBaseBolt()
    buildAnomalousTruckingEventsHBaseBolt()
    buildAnomalousTruckingEventsCountHBaseBolt()

    // Build KafkaStore Bolt for pushing values to a messaging hub
    buildKafkaBolt()

    logger.info("Storm topology finished building.")

    // Finally, create the topology
    builder.createTopology()
  }

  def buildKafkaSpout()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val hosts = new ZkHosts(config.getString(Config.STORM_ZOOKEEPER_SERVERS))
    val zkRoot = config.getString(Config.STORM_ZOOKEEPER_ROOT)
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)
    val topic = config.getString(TruckingTopology.TruckGeoTopic)
    val groupId = config.getString(TruckingTopology.ConsumerGroupId)

    // Create a Spout configuration object and apply the scheme for the data that will come through this spout
    val spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId)
    spoutConfig.scheme = new SchemeAsMultiScheme(TruckingEventScheme)

    // Create a spout with the specified configuration, and place it in the topology blueprint
    val kafkaSpout = new KafkaSpout(spoutConfig)
    builder.setSpout("truckingEvents", kafkaSpout, taskCount)
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
      .fieldsGrouping("truckingEvents", new Fields("driverId")) // TODO: cleanup/remove Field
  }

  def buildAllTruckingEventsHBaseBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // Create an HBaseMapper that maps Storm tuples to HBase columns
    val mapper = new SimpleHBaseMapper()
      .withRowKeyField(config.getString(TruckingTopology.EventKeyField))
      .withColumnFields(new Fields()) // TODO: implement
      .withColumnFamily(config.getString(TruckingTopology.ColumnFamily))

    // Create a bolt, with its configurations stored under the configuration keyed "emptyConfig"
    // Default configs here: https://github.com/apache/hbase/blob/master/hbase-common/src/main/resources/hbase-default.xml
    val bolt = new HBaseBolt(config.getString(TruckingTopology.AllTruckingEvents), mapper)
      .withConfigKey("emptyConfig")

    // Place the bolt in the topology builder
    builder.setBolt("persistAllTruckingEvents", bolt, taskCount).shuffleGrouping("joinTruckEvents")
      //.fieldsGrouping("joinTruckEvents", getAllFields()) // TODO: what?
  }

  def buildAnomalousTruckingEventsHBaseBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // Create an HBaseMapper that maps Storm tuples to HBase columns
    val mapper = new SimpleHBaseMapper()
      .withRowKeyField(TruckingTopology.EventKeyField)
      .withColumnFields(new Fields()) // TODO: implement
      .withColumnFamily(config.getString(TruckingTopology.ColumnFamily))

    // Create a bolt, with its configurations stored under the configuration keyed "emptyConfig"
    val bolt = new HBaseBolt(config.getString(TruckingTopology.AnomalousTruckingEvents), mapper)
      .withConfigKey("emptyConfig")

    // Place the bolt in the topology builder
    builder.setBolt("persistAnomalousTruckingEvents", bolt, taskCount)
      .shuffleGrouping("joinTruckEvents", "anomalousEvents")
  }

  def buildAnomalousTruckingEventsCountHBaseBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // Create an HBaseMapper that maps Storm tuples to HBase columns
    val mapper = new SimpleHBaseMapper()
      .withRowKeyField(TruckingTopology.EventKeyField)
      .withColumnFields(new Fields()) // TODO: implement
      //.withCounterFields(new Fields("")) // TODO: implement
      .withColumnFamily(config.getString(TruckingTopology.ColumnFamily))

    // Create a bolt, with its configurations stored under the configuration keyed "emptyConfig"
    val bolt = new HBaseBolt(config.getString(TruckingTopology.AnomalousTruckingEventsCount), mapper)
      .withConfigKey("emptyConfig")

    // Place the bolt in the topology builder
    builder.setBolt("persistAnomalousTruckingEventsCount", bolt, taskCount)
      .shuffleGrouping("joinTruckEvents", "anomalousEvents")  // TODO: for now, this looks exactly like anomalousTable ... implement counting
  }

  def buildKafkaBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)
    val bootstrapServers = config.getString(TruckingTopology.KafkaBootstrapServers)
    val keySerializer = config.getString(TruckingTopology.KafkaKeySerializer)
    val valueSerializer = config.getString(TruckingTopology.KafkaValueSerializer)

    // Define properties to pass along to the KafkaBolt
    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapServers)
    props.setProperty("key.serializer", keySerializer)
    props.setProperty("value.serializer", valueSerializer)

    val bolt = new KafkaBolt()
      .withTopicSelector(new DefaultTopicSelector(config.getString(TruckingTopology.TruckingTopic)))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper()) // TODO: when first imported, inteillj added "[]" for types to fill in
      .withProducerProperties(props)

    builder.setBolt("pushOutTruckingEvents", bolt, taskCount)
      .shuffleGrouping("joinTruckEvents")
  }
}
