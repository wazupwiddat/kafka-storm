package com.learningkafka

import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import org.apache.commons.lang.StringUtils
import org.apache.storm.guava.collect.ImmutableList
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by jwarren on 1/25/15.
 */
public class SentenceBolt extends BaseBasicBolt{
  private List<String> words = new ArrayList<String>()
  private static final Logger LOG = LoggerFactory.getLogger(SentenceBolt.class)

  @Override
  void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String word = tuple.getString(0)
    if (StringUtils.isBlank(word)) {
      // ignore blank lines
      LOG.info("Ignoring blank lines")
      return;
    }

    LOG.info("Received word: " + word)
    words.add(word)
    if (word.endsWith(".")) {
      // end of sentence
      LOG.info("emitting sentence")
      basicOutputCollector.emit(ImmutableList.of((Object) StringUtils.join(words, ' ')))

      // clear words collection
      words.clear()
    }

  }

  @Override
  void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"))
  }
}
