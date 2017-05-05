package heron.starter.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSampleSpout extends BaseRichSpout 
{
    SpoutOutputCollector _collector;
    LinkedBlockingQueue _msgs = null;
    TwitterStream _twitterStream;

    String _consumerKey;
    String _consumerSecret;
    String _accessToken;
    String _accessTokenSecret;
    
    public TwitterSampleSpout(String consumerKey, String consumerSecret) {
        if (consumerKey == null ||
            consumerSecret == null) {
            throw new RuntimeException("Twitter4j OAuth fields cannot be null");
        }

        _consumerKey = consumerKey;
        _consumerSecret = consumerSecret;
    }

    public TwitterSampleSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        if (consumerKey == null ||
            consumerSecret == null ||
            accessToken == null ||
            accessTokenSecret == null) {
            throw new RuntimeException("Twitter4j OAuth fields cannot be null");
        }

        _consumerKey = consumerKey;
        _consumerSecret = consumerSecret;
        _accessToken = accessToken;
        _accessTokenSecret = accessTokenSecret;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _msgs = new LinkedBlockingQueue();
        _collector = collector;

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                _msgs.offer(status.getText());
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }

            @Override
            public void onException(Exception e) {
              e.printStackTrace();
            }
        };

        ConfigurationBuilder config = 
            new ConfigurationBuilder()
                 .setOAuthConsumerKey(_consumerKey)
                 .setOAuthConsumerSecret(_consumerSecret)
                 .setOAuthAccessToken(_accessToken)
                 .setOAuthAccessTokenSecret(_accessTokenSecret);

        TwitterStreamFactory fact = 
            new TwitterStreamFactory(config.build());

        _twitterStream = fact.getInstance();
        _twitterStream.addListener(listener);
        _twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Object ret = _msgs.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            System.out.println(" ret: " + ret);
            _collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
        super.close();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }    

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}
