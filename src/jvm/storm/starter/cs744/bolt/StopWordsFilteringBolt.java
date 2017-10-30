package storm.starter.cs744.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.starter.cs744.util.Constants;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StopWordsFilteringBolt extends BaseRichBolt {
    private final Set<String> stopWords;
    private OutputCollector collector;

    public StopWordsFilteringBolt(String[] stopWordsArr) {
        this.stopWords = new HashSet<>();
        for (String s : stopWordsArr) {
            stopWords.add(s.toLowerCase().trim());
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String[] words = ((String) input.getValue(0)).split("\\s+");
        for (String word : words) {
            //To replace all punctuation marks otherwise same word was coming
            // in two different formats: bob, and bob
            word = word.replaceAll("[^a-zA-Z0-9\\-]", "");
            word = word.toLowerCase();
            word = word.trim();
            if (allowWord(word)) {
                collector.emit(new Values(word));
            }
        }
    }

    private boolean allowWord(String word) {
        return !stopWords.contains(word);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.TWEET_WORD_FIELD));
    }
}
