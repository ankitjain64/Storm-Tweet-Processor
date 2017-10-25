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
            //Can result in a skew
            if (Long.compare(ts, currentTimeStamp) != 0) {
                StringBuilder sb = new StringBuilder();
                sb.append(currentTimeStamp).append(",");
                Collections.sort(currentBatch, new Comparator<WordFrequency>() {
                    @Override
                    public int compare(WordFrequency o1, WordFrequency o2) {
                        return o1.getFrequency() - o2.getFrequency();
                    }
                });


                for (int i = currentBatch.size() - 1; i > (currentBatch.size() / 2); i--) {
                    WordFrequency entry = currentBatch.get(i);
                    sb.append(entry.getWord()).append("_").append(entry.getFrequency()).append("-:-");

                }
                collector.emit(new Values(sb.toString()));
                currentTimeStamp = ts;
                currentBatch = new ArrayList<>();
                currentBatch.add(frequency);
            } else {
                currentBatch.add(frequency);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.POPULAR_WORDS_FIELD));
    }
}
