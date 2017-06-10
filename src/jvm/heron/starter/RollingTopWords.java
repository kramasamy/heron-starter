package heron.starter;

import java.util.LinkedList;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.twitter.heron.common.basics.ByteAmount;

import heron.starter.bolt.IntermediateRankingsBolt;
import heron.starter.bolt.RollingCountBolt;
import heron.starter.bolt.TotalRankingsBolt;
import heron.starter.tools.RankableObjectWithFields;
import heron.starter.tools.Rankings;
import heron.starter.util.StormRunner;
import heron.starter.spout.WordSpout;


/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class RollingTopWords {
  private static final String MODE_OF_OPERATION_CLUSTER = "Cluster";
  private static final String MODE_OF_OPERATION_LOCAL = "Local";
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int NUMBER_OF_STREAM_MANAGERS = 50;
  private static final int TOP_N = 5;

  private static final String spoutId = "wordGenerator";
  private static final String counterId = "counter";
  private static final String intermediateRankerId = "intermediateRanker";
  private static final String totalRankerId = "finalRanker";

  private final TopologyBuilder builder;
  private final String mode;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public RollingTopWords(String topologyName, String mode) throws InterruptedException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    this.mode = mode;
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumStmgrs(NUMBER_OF_STREAM_MANAGERS);
    conf.setContainerCpuRequested(2.0f);
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(3));

    com.twitter.heron.api.Config.setComponentRam(conf, spoutId, ByteAmount.fromMegabytes(256));
    com.twitter.heron.api.Config.setComponentRam(conf, counterId, ByteAmount.fromMegabytes(256));
    com.twitter.heron.api.Config.setComponentRam(conf, intermediateRankerId, ByteAmount.fromMegabytes(256));
    com.twitter.heron.api.Config.setComponentRam(conf, totalRankerId, ByteAmount.fromMegabytes(256));

    conf.registerSerialization(Rankings.class);
    conf.registerSerialization(RankableObjectWithFields.class);
    conf.registerSerialization(LinkedList.class);
    return conf;
  }

  private void wireTopology() throws InterruptedException {
    builder.setSpout(spoutId, new WordSpout(), NUMBER_OF_STREAM_MANAGERS);
    builder.setBolt(counterId, new RollingCountBolt(9, 3), NUMBER_OF_STREAM_MANAGERS - 1).fieldsGrouping(spoutId, new Fields("word"));
    builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), NUMBER_OF_STREAM_MANAGERS - 1).fieldsGrouping(counterId, new Fields("obj"));
    builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
  }

  public void run() throws InterruptedException, InvalidTopologyException, AlreadyAliveException, NotAliveException {
    if (mode != null && mode.equals(MODE_OF_OPERATION_CLUSTER)) {
        StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
    } else {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }
  }

    public static void main(String[] args) throws Exception {
        if (args != null) {
            if (args.length < 2) {
                Exception exception = new IllegalArgumentException("Illegal number of command line arguments supplied.\nPlease provide the topologyName as the first argument and either 'Cluster' or 'Local' as the second argument.");
                throw exception;
            }

            if (!args[1].equals(MODE_OF_OPERATION_CLUSTER) && !args[1].equals(MODE_OF_OPERATION_LOCAL)) {
                Exception exception = new IllegalArgumentException("The allowed values for the second argument is either 'Cluster' or 'Local'.  Please provide a valid value for the second argument.");
                throw exception;
            }

            new RollingTopWords(args[0], args[1]).run();
        }
    }
}
