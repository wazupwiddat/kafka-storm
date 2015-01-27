package com.learningkafka

import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Tuple
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by jwarren on 1/25/15.
 */
public class PrinterBolt extends BaseBasicBolt{
  private static final Logger LOG = LoggerFactory.getLogger(PrinterBolt.class)

  @Override
  void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String sentence = tuple.getString(0)
    LOG.info("Received Sentence: " + sentence)
  }

  @Override
  void declareOutputFields(OutputFieldsDeclarer declarer) {
    // nothing to emit, we are done
  }
}
