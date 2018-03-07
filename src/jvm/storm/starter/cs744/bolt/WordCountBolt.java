package storm.starter.cs744.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.starter.cs744.WordFrequency;
import storm.starter.cs744.util.Utils;

import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS;
import static storm.starter.cs744.util.Constants.*;
import static storm.starter.cs744.util.Utils.isTickTuple;

/**
 * Emits word count every 30 seconds
 */
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> wordVsWordCount;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.wordVsWordCount = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        if (isTickTuple(input)) {
            //append correct time
            long currentTime = Utils.getCurrentTime();
            for (Map.Entry<String, Integer> entry : wordVsWordCount.entrySet()) {
                String word = entry.getKey();
                Integer count = entry.getValue();
                collector.emit(new Values(currentTime, new WordFrequency(word, count)));
            }
            wordVsWordCount = new HashMap<>();
        } else {
            String word = (String) input.getValue(0);
            Integer count = wordVsWordCount.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            wordVsWordCount.put(word, count);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TS_FIELD, WORD_FREQUENCY_FIELD));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(TOPOLOGY_TICK_TUPLE_FREQ_SECS, WORD_COUNT_EMIT_FREQUENCY);
        return conf;
    }
}
