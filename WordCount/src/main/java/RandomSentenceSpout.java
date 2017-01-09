import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by ponteru07 on 2017/01/09.
 */
public class RandomSentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private Random _rnd;

    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        _collector = collector;
        _rnd = new Random();
    }

    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences = new String[] {"the cow jumped over the moon", "an apple a day keeps doctor away",
                                           "four score and seven years ago", "snow white and seven dwarfs"};
        String sentence = sentences[_rnd.nextInt(sentences.length)];
        _collector.emit(new Values(sentence));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
