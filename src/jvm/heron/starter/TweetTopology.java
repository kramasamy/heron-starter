package heron.starter;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import heron.starter.spout.TwitterSampleSpout;
import heron.starter.util.StormRunner;


/**
 * This is a basic example of a Storm topology.
 */
public class TweetTopology {
    private static final String MODE_OF_OPERATION_CLUSTER = "Cluster";
    private static final String MODE_OF_OPERATION_LOCAL = "Local";
    private static final int NUMBER_OF_WORKERS = 3;  //default value
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;

    private static String consumerKey = "UPDATE YOUR VALUE HERE";
    private static String consumerSecret = "UPDATE YOUR VALUE HERE";
    private static String accessToken = "UPDATE YOUR VALUE HERE";
    private static String accessTokenSecret = "UPDATE YOUR VALUE HERE";

  public static class StdoutBolt extends BaseRichBolt {
    OutputCollector _collector;
    String taskName;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      taskName = context.getThisComponentId() + "_" + context.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {
      System.out.println(tuple.getValue(0));
      _collector.emit(tuple, new Values(tuple.getValue(0)));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
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


            String topologyName = args[0];

            TopologyBuilder builder = new TopologyBuilder();
            
            builder.setSpout("word", 
                             new TwitterSampleSpout(consumerKey,
                                                    consumerSecret,
                                                    accessToken,
                                                    accessTokenSecret),
                             1);

            builder.setBolt("stdout", new StdoutBolt(), 3).shuffleGrouping("word");


            Config conf = new Config();
            conf.setDebug(true);
            conf.setNumWorkers(NUMBER_OF_WORKERS);

            if (args[1].equals(MODE_OF_OPERATION_CLUSTER)) {
                StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, conf);
            } else {
                StormRunner.runTopologyLocally(builder.createTopology(), topologyName, conf, DEFAULT_RUNTIME_IN_SECONDS);
            }
        } 
    }
}
