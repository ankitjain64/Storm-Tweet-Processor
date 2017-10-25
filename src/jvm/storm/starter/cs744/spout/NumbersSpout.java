package storm.starter.cs744.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import storm.starter.cs744.util.Constants;

import java.util.Map;
import java.util.Set;

import static storm.starter.cs744.util.Constants.SAMPLED_NUMBER_FIELD;
import static storm.starter.cs744.util.Utils.doSample;
import static storm.starter.cs744.util.Utils.getCurrentTime;

/**
 * Generates a number n for friends filtering bolt
 */
public class NumbersSpout extends BaseRichSpout {
    private final Integer[] numbers;
    private SpoutOutputCollector collector;
    private int sampleCount;

    public NumbersSpout(Integer[] numbers) {
        super();
        this.numbers = numbers;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        long currentTime = getCurrentTime();
        Set<Integer> sampledData = doSample(currentTime, 1, numbers);
        collector.emit(new Values(sampledData.iterator().next()));
        Utils.sleep(Constants.SAMPLED_DATA_EMIT_FREQUENCY);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SAMPLED_NUMBER_FIELD));
    }
}
