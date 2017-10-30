package storm.starter.cs744.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import storm.starter.cs744.WordFrequency;
import storm.starter.cs744.util.Utils;

import java.util.*;

import static storm.starter.cs744.util.Constants.POPULAR_WORDS_FIELD;
import static storm.starter.cs744.util.Constants.WORD_COUNT_EMIT_FREQUENCY;

public class PopularWordsBolt extends BaseRichBolt {

    private static final long serialVersionUID = 4931640198501530202L;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = WORD_COUNT_EMIT_FREQUENCY;

    private final int emitFrequencyInSeconds;
    private List<WordFrequency> wordFrequencies;
    private OutputCollector collector;

    public PopularWordsBolt() {
        this(DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public PopularWordsBolt(int emitFrequencyInSeconds) {
        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException(
                    "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        wordFrequencies = new ArrayList<>();
    }

    private void emitRankings() {
        Collections.sort(wordFrequencies, new Comparator<WordFrequency>() {
            @Override
            public int compare(WordFrequency o1, WordFrequency o2) {
                return Long.compare(o1.getFrequency(), o2.getFrequency());
            }
        });
        long currentTime = Utils.getCurrentTime();
        for (int i = wordFrequencies.size() - 1; i >= wordFrequencies.size() / 2; i--) {
            WordFrequency frequency = wordFrequencies.get(i);
            StringBuilder sb = new StringBuilder();
            sb.append(currentTime).append(",").append(frequency.getWord())
                    .append(",").append(frequency.getFrequency());
            collector.emit(new Values(sb.toString()));
        }
        wordFrequencies = new ArrayList<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            emitRankings();
        } else {
            wordFrequencies.add((WordFrequency) input.getValue(0));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(POPULAR_WORDS_FIELD));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
