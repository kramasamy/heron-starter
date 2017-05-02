package heron.starter;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import heron.starter.util.StormRunner;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {
  private static final String MODE_OF_OPERATION_CLUSTER = "Cluster";
  private static final String MODE_OF_OPERATION_LOCAL = "Local";
  private static final int NUMBER_OF_WORKERS = 3;  //default value
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;

  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;
    String myId;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      myId = context.getThisComponentId();
    }

    @Override
    public void execute(Tuple tuple) {
      String msg = tuple.getString(0) + "!!!";
      _collector.emit(tuple, new Values(msg));
      _collector.ack(tuple);
      System.out.println(myId + ": " + msg);
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

            builder.setSpout("word", new TestWordSpout(), 10);
            builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
            builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
    

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
