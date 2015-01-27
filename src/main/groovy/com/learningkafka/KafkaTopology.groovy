package com.learningkafka

import backtype.storm.Config
import backtype.storm.StormSubmitter
import backtype.storm.spout.SchemeAsMultiScheme
import backtype.storm.topology.TopologyBuilder
import kafka.server.KafkaConfig
import storm.kafka.KafkaSpout
import storm.kafka.SpoutConfig
import storm.kafka.StringScheme
import storm.kafka.ZkHosts

/**
 * Created by jwarren on 1/25/15.
 */
public class KafkaTopology {
  public static void main(String[] args) {

    // Zookeeper hosts default port is 2181 and routes from the docker instance as 49181
    //  DOCKER_HOST : 49181:2181
    ZkHosts zkHosts = new ZkHosts("192.168.59.103:49181")

    // Setup the config for the topic that the KafkaSpout will receive messages from
    SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "words_topic", "", "id7")

    // messages are string
    kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme())

    // We want to consume all the first messages in
    // the topic every time we run the topology to
    // help in debugging. In production, this
    // property should be false
    kafkaConfig.forceFromStart = true;

    TopologyBuilder builder = new TopologyBuilder()

    // Kafka spout
    builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1)

    // Sentence bolt
    builder.setBolt("SentenceBolt", new SentenceBolt(), 1).globalGrouping("KafkaSpout")
    builder.setBolt("PrinterBolt", new PrinterBolt(), 1).globalGrouping("SentenceBolt")

    // Submit topology
    Config conf = new Config();
    conf.setDebug(true)
    StormSubmitter.submitTopology(args[0], conf, builder.createTopology())
  }
}
