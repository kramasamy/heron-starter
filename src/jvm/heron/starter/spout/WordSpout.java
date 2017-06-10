package heron.starter.spout;

import java.util.Map;
import java.util.Random;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * A spout that emits a random word
 */
public class WordSpout extends BaseRichSpout {
  private static final long serialVersionUID = 4322775001819135036L;

  private static final int ARRAY_LENGTH = 128 * 1024;
  private static final int WORD_LENGTH = 20;

  private final String[] words = new String[ARRAY_LENGTH];
  private final Random rnd = new Random(31);
  private SpoutOutputCollector collector;


  // Utils class to generate random String at given length
  public static class RandomString {
    private final char[] symbols;
    private final Random random = new Random();
    private final char[] buf;

    public RandomString(int length) {
      // Construct the symbol set
      StringBuilder tmp = new StringBuilder();
      for (char ch = '0'; ch <= '9'; ++ch) {
        tmp.append(ch);
      }

      for (char ch = 'a'; ch <= 'z'; ++ch) {
        tmp.append(ch);
      }

      symbols = tmp.toString().toCharArray();
      if (length < 1) {
        throw new IllegalArgumentException("length < 1: " + length);
      }

      buf = new char[length];
    }

    public String nextString() {
      for (int idx = 0; idx < buf.length; ++idx) {
        buf[idx] = symbols[random.nextInt(symbols.length)];
      }

      return new String(buf);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
    RandomString randomString = new RandomString(WORD_LENGTH);
    for (int i = 0; i < ARRAY_LENGTH; i++) {
      words[i] = randomString.nextString();
    }

    collector = spoutOutputCollector;
  }

  @Override
  public void nextTuple() {
    int nextInt = rnd.nextInt(ARRAY_LENGTH);
    collector.emit(new Values(words[nextInt]));
  }
}
