package storm.starter.cs744.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.starter.cs744.WordFrequency;
import storm.starter.cs744.util.Constants;

import java.util.*;

/**
 * Emits a string of popular words_count seperated by -:-
 */
public class PopularWordsBolt extends BaseRichBolt {

    private OutputCollector collector;
    private List<WordFrequency> currentBatch;
    private Long currentTimeStamp;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.currentBatch = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        Long ts = (Long) input.getValue(0);
        WordFrequency frequency = (WordFrequency) input.getValue(1);
        if (currentTimeStamp == null) {
            currentTimeStamp = ts;
            currentBatch.add(frequency);
        } else {
            int delta = 5000; //to handle clock skew ts+delta and ts-delta since
            // word filter had a tick of 30s, 5000 ms is a reasonable delta
            if (Long.compare(ts + delta, currentTimeStamp) >= 0 && Long.compare
                    (ts - delta, currentTimeStamp) <= 0) {
                currentBatch.add(frequency);
            } else {
                Collections.sort(currentBatch, new Comparator<WordFrequency>() {
                    @Override
                    public int compare(WordFrequency o1, WordFrequency o2) {
                        return o1.getFrequency() - o2.getFrequency();
                    }
                });

                StringBuilder sb;
                for (int i = currentBatch.size() - 1; i >= (currentBatch.size() / 2); i--) {
                    sb = new StringBuilder().append(currentTimeStamp).append(",");
                    WordFrequency entry = currentBatch.get(i);
                    sb.append(entry.getWord()).append(",").append(entry.getFrequency());
                    collector.emit(new Values(sb.toString()));
                }
                currentTimeStamp = ts;
                currentBatch = new ArrayList<>();
                currentBatch.add(frequency);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.POPULAR_WORDS_FIELD));
    }
}
