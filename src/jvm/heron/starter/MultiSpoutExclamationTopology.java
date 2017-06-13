package heron.starter;

import java.util.Map;

import com.twitter.heron.common.basics.ByteAmount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.api.GlobalMetrics;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Values;

import heron.starter.bolt.IntermediateRankingsBolt;
import heron.starter.bolt.RollingCountBolt;
import heron.starter.bolt.TotalRankingsBolt;
import heron.starter.tools.RankableObjectWithFields;
import heron.starter.tools.Rankings;
import heron.starter.util.StormRunner;
import heron.starter.spout.WordSpout;

/**
 * This is a basic example of a Storm topology.
 */
public final class MultiSpoutExclamationTopology {

  private MultiSpoutExclamationTopology() {
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word0", new WordSpout(), 5);
    builder.setSpout("word1", new WordSpout(), 5);
    builder.setSpout("word2", new WordSpout(), 5);
    builder.setBolt("exclaim1", new ExclamationBolt(), 10)
        .shuffleGrouping("word0")
        .shuffleGrouping("word1")
        .shuffleGrouping("word2");
    builder.setBolt("exclaim2", new ExclamationBolt2(), 10)
        .shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    com.twitter.heron.api.Config.setComponentRam(conf, "word0", ByteAmount.fromMegabytes(500));
    com.twitter.heron.api.Config.setComponentRam(conf, "word1", ByteAmount.fromMegabytes(500));
    com.twitter.heron.api.Config.setComponentRam(conf, "word2", ByteAmount.fromMegabytes(500));
    com.twitter.heron.api.Config.setComponentRam(conf, "exclaim1", ByteAmount.fromGigabytes(1));

    if (args != null && args.length > 0) {
      conf.setNumStmgrs(10);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }

  public static class ExclamationBolt extends BaseRichBolt {
    private static final long serialVersionUID = 6945654705222426596L;
    private long nItems;
    private long startTime;
    private OutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf,
                        TopologyContext context, OutputCollector collector) {
      nItems = 0;
      startTime = System.currentTimeMillis();
      this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      System.out.println(tuple.getString(0));
      collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      collector.ack(tuple);
      if (++nItems % 100000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
        GlobalMetrics.incr("selected_items");
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class ExclamationBolt2 extends BaseRichBolt {
    private static final long serialVersionUID = 6945654705222426596L;
    private long nItems;
    private long startTime;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf,
                        TopologyContext context, OutputCollector collector) {
      nItems = 0;
      startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
      // System.out.println(tuple.getString(0));
      // collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      // collector.ack(tuple);
      if (++nItems % 100000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
        GlobalMetrics.incr("selected_items");
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      // declarer.declare(new Fields("word"));
    }
  }
}
