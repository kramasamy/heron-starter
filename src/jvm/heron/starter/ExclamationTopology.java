package heron.starter;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
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
  private static final int NUMBER_OF_WORKERS = 3;  //default value

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
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);

    String topologyName = "SampleTopology1";  //default value

    if (args != null && args.length > 0) {
        conf.setNumWorkers(NUMBER_OF_WORKERS);

        topologyName = args[0];

        if (args.length == 2) {
            if (args[1].equals("Cluster")) {
                StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, conf);
            } else {
                Exception exception = new IllegalArgumentException("The allowed values for the second argument is either 'Cluster' or 'Local'.  Please provide a valid value for the second argument.");
                throw exception;
            }
        } else {
            submitTopologyToLocalCluster(topologyName, conf, builder);
        }
    }
    else {
        submitTopologyToLocalCluster(topologyName, conf, builder);
    }
  }

    private static void submitTopologyToLocalCluster(String topologyName, Config conf, TopologyBuilder builder) throws AlreadyAliveException, InvalidTopologyException, NotAliveException {
        if (topologyName != null && topologyName.length() > 0 && conf != null && builder != null) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }
}
