package com.hortonworks.orendainx.truck.topology

import com.typesafe.config.ConfigFactory
import com.typesafe.config.{Config => TypeConfig}
import java.util.Properties

import com.hortonworks.orendainx.truck.topology.bolts.TruckGeoSpeedJoinBolt
import com.hortonworks.orendainx.truck.topology.schemes.{TruckGeoScheme, TruckSpeedScheme}
import org.apache.storm.generated.StormTopology
import org.apache.storm.{Config, StormSubmitter}
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

import scala.concurrent.duration._
import org.apache.hadoop.hbase.HBaseConfiguration

import scala.collection.immutable.HashMap

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
  val TruckGeoSpeedTopic = "kafka.consumer.truck-geospeed.topic"

  // Kafka producer configuration constants
  val KafkaBootstrapServers = "kafka.producer.bootstrap-servers"
  val KafkaKeySerializer = "kafka.producer.key-serializer"
  val KafkaValueSerializer = "kafka.producer.value-serializer"

  // HBase constants
  val EventKeyField = "hbase.event-key-field" // TODO: shared with model classes
  val ColumnFamily = "hbase.column-family"
  val AllTruckGeoSpeedEvents = "hbase.all-truck-geospeed.table"
  val AnomalousTruckGeoSpeedEvents = "hbase.anomalous-truck-geospeed.table"
  val AnomalousTruckGeoSpeedEventsCount = "hbase.anomalous-truck-geospeed-count.table"


  def main(args: Array[String]): Unit = {

    // Extract specified configuration file path from supplied argument, else throw Exception
    val configName = if (args.nonEmpty) args(0) else throw new IllegalArgumentException("Must specify path to configuration file.")
    val config = ConfigFactory.load(configName)

    // Set up configuration for the Storm Topology
    val stormConfig = new Config()
    stormConfig.setDebug(config.getBoolean(Config.TOPOLOGY_DEBUG))
    stormConfig.setMessageTimeoutSecs(config.getInt(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS))
    stormConfig.setNumWorkers(config.getInt(Config.TOPOLOGY_WORKERS))
    //val emptyConfig = Map.empty[String, String]
    val emptyConfig = scala.collection.immutable.HashMap[String, String](("A", "B")).map(identity)
    stormConfig.put("emptyConfig", emptyConfig) // Because current version of storm is the way it is -_-

    // Build out a TruckTopology
    val topology = new TruckingTopology(config).buildTopology()

    // TODO: diff between this and cluster modes
    // TODO: for reference: https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.5.0/bk_storm-component-guide/content/storm-kafkaspout-config-core.html
    // TODO: good info: http://kafka.apache.org/documentation.html#newconsumerconfigs
    // TODO: read https://github.com/apache/storm/tree/master/external/storm-kafka
    // TODO: https://cwiki.apache.org/confluence/display/KAFKA/FAQ

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

  /**
    *
    * @return a built StormTopology
    */
  def buildTopology(): StormTopology = {
    // Builder to perform the construction of the topology.
    val builder = new TopologyBuilder()

    // Build Kafka Spouts to ingest truck geo/speed events
    buildTruckGeoSpout()
    buildTruckSpeedSpout()

    // Built Bolt to Join geo and speed events
    buildStreamJoinBolt()

    // Build HBase Bolts to persist all events, as well as specific anomalies
    buildAllTruckGeoSpeedEventsHBaseBolt()
    buildAnomalousTruckGeoSpeedEventsHBaseBolt()
    buildAnomalousTruckGeoSpeedEventsCountHBaseBolt()

    // Build KafkaStore Bolt for pushing values to a messaging hub
    buildTruckGeoSpeedKafkaBolt()

    // Finally, create the topology
    builder.createTopology()
  }

  def buildTruckGeoSpout()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val hosts = new ZkHosts(config.getString(Config.STORM_ZOOKEEPER_SERVERS))
    val zkRoot = config.getString(Config.STORM_ZOOKEEPER_ROOT)
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)
    val topic = config.getString(TruckingTopology.TruckGeoTopic)
    val groupId = config.getString(TruckingTopology.ConsumerGroupId)

    // Create a Spout configuration object and apply the scheme for the data that will come through this spout
    val spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId)
    spoutConfig.scheme = new SchemeAsMultiScheme(TruckGeoScheme)

    // Create a spout with the specified configuration, and place it in the topology blueprint
    val kafkaSpout = new KafkaSpout(spoutConfig)
    builder.setSpout("truckGeoEvents", kafkaSpout, taskCount)
  }

  def buildTruckSpeedSpout()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val hosts = new ZkHosts(config.getString(Config.STORM_ZOOKEEPER_SERVERS))
    val zkRoot = config.getString(Config.STORM_ZOOKEEPER_ROOT)
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)
    val topic = config.getString(TruckingTopology.TruckSpeedTopic)
    val groupId = config.getString(TruckingTopology.ConsumerGroupId)

    // Create a Spout configuration object and apply the scheme for the data that will come through this spout
    val spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId)
    spoutConfig.scheme = new SchemeAsMultiScheme(TruckSpeedScheme)
    spoutConfig.ignoreZkOffsets = true // Force the spout to ignore where it left off during previous runs

    // Create a spout with the specified configuration, and place it in the topology blueprint
    val kafkaSpout = new KafkaSpout(spoutConfig)
    builder.setSpout("truckSpeedEvents", kafkaSpout, taskCount)
  }

  def buildStreamJoinBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)
    val duration = config.getInt(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)

    // Create a bolt with a tumbling window
    val windowDuration = new BaseWindowedBolt.Duration(duration, MILLISECONDS)
    val bolt = new TruckGeoSpeedJoinBolt().withTumblingWindow(windowDuration)

    // Place the bolt in the topology blueprint
    builder.setBolt("joinTruckEvents", bolt, taskCount)
      .fieldsGrouping("truckGeoEvents", new Fields("driverId")) // TODO: cleanup/remove Fields
      .fieldsGrouping("truckSpeedEvents", new Fields("driverId"))
  }

  def buildAllTruckGeoSpeedEventsHBaseBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // Create an HBaseMapper that maps Storm tuples to HBase columns
    val mapper = new SimpleHBaseMapper()
      .withRowKeyField(config.getString(TruckingTopology.EventKeyField))
      .withColumnFields(new Fields()) // TODO: implement
      .withColumnFamily(config.getString(TruckingTopology.ColumnFamily))

    // Create a bolt, with its configurations stored under the configuration keyed "emptyConfig"
    val bolt = new HBaseBolt(config.getString(TruckingTopology.AllTruckGeoSpeedEvents), mapper)
      .withConfigKey("emptyConfig")

    // Place the bolt in the topology builder
    builder.setBolt("persistAllTruckGeoSpeedEvents", bolt, taskCount).shuffleGrouping("joinTruckEvents")
      //.fieldsGrouping("joinTruckEvents", getAllFields()) // TODO: what?
  }

  def buildAnomalousTruckGeoSpeedEventsHBaseBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // Create an HBaseMapper that maps Storm tuples to HBase columns
    val mapper = new SimpleHBaseMapper()
      .withRowKeyField(TruckingTopology.EventKeyField)
      .withColumnFields(new Fields()) // TODO: implement
      .withColumnFamily(config.getString(TruckingTopology.ColumnFamily))

    // Create a bolt, with its configurations stored under the configuration keyed "emptyConfig"
    val bolt = new HBaseBolt(config.getString(TruckingTopology.AnomalousTruckGeoSpeedEvents), mapper)
      .withConfigKey("emptyConfig")

    // Place the bolt in the topology builder
    builder.setBolt("persistAnomalousTruckGeoSpeedEvents", bolt, taskCount)
      .shuffleGrouping("joinTruckEvents", "anomalousEvents")
  }

  def buildAnomalousTruckGeoSpeedEventsCountHBaseBolt()(implicit builder: TopologyBuilder): Unit = {
    // Extract values from config
    val taskCount = config.getInt(Config.TOPOLOGY_TASKS)

    // Create an HBaseMapper that maps Storm tuples to HBase columns
    val mapper = new SimpleHBaseMapper()
      .withRowKeyField(TruckingTopology.EventKeyField)
      .withColumnFields(new Fields()) // TODO: implement
      //.withCounterFields(new Fields("")) // TODO: implement
      .withColumnFamily(config.getString(TruckingTopology.ColumnFamily))

    // Create a bolt, with its configurations stored under the configuration keyed "emptyConfig"
    val bolt = new HBaseBolt(config.getString(TruckingTopology.AnomalousTruckGeoSpeedEventsCount), mapper)
      .withConfigKey("emptyConfig")

    // Place the bolt in the topology builder
    builder.setBolt("persistAnomalousTruckGeoSpeedEventsCount", bolt, taskCount)
      .shuffleGrouping("joinTruckEvents", "anomalousEvents")  // TODO: for now, this looks exactly like anomalousTable ... implement counting
  }

  def buildTruckGeoSpeedKafkaBolt()(implicit builder: TopologyBuilder): Unit = {
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
      .withTopicSelector(new DefaultTopicSelector(config.getString(TruckingTopology.TruckGeoSpeedTopic)))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper()) // TODO: when first imported, inteillj added "[]" for types to fill in
      .withProducerProperties(props)

    builder.setBolt("pushOutTruckGeoSpeedEvents", bolt, taskCount)
      .shuffleGrouping("joinTruckEvents")
  }
}
