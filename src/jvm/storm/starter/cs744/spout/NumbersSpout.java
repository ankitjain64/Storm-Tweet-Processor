package storm.starter.cs744.spout;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;

import static storm.starter.cs744.util.Constants.SAMPLED_DATA_EMIT_FREQUENCY;
import static storm.starter.cs744.util.Constants.SAMPLED_NUMBER_FIELD;
import static storm.starter.cs744.util.Utils.doSample;
import static storm.starter.cs744.util.Utils.getCurrentTime;


/**
 * Generates a number n for friends filtering bolt
 */

/**
 * Cannot call Thread.sleep in next tuple
 * refer: https://groups.google.com/forum/#!topic/storm-user/kM2O1gT4lPs
 */
public class NumbersSpout extends BaseRichSpout {
    private final Integer[] numbers;
    private SpoutOutputCollector collector;
    private Long lastEmitTime;

    public NumbersSpout(Integer[] numbers) {
        super();
        this.numbers = numbers;
        this.lastEmitTime = null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * This method needs to be non-blocking
     */
    @Override
    public void nextTuple() {
        long currentTime = getCurrentTime();
        System.out.println("1_" + currentTime);
        if (this.lastEmitTime == null) {
//            System.out.println(currentTime + "_true");
            doEmit(currentTime);
        } else {
            if (Long.compare(currentTime, lastEmitTime + SAMPLED_DATA_EMIT_FREQUENCY) > 0) {
//                System.out.println(currentTime + "_true2");
                doEmit(currentTime);
            } else {
//                System.out.println(currentTime + "_false");
                Utils.sleep(50);//To save CPU Cycle
            }
        }
    }

    private void doEmit(long currentTime) {
        List<Integer> sampledData = doSample(currentTime, 1, numbers);
        collector.emit(new Values(sampledData.get(0)));
        this.lastEmitTime = currentTime;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SAMPLED_NUMBER_FIELD));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

}
