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

import static storm.starter.cs744.util.Constants.SAMPLED_HASHTAGS_FIELD;
import static storm.starter.cs744.util.Utils.doSample;
import static storm.starter.cs744.util.Utils.getCurrentTime;

/**
 * Generates a sample set of hashtags
 */
public class SampledHashTagsSpout extends BaseRichSpout {

    private String[] hashTags;
    private int sampleCount;
    private SpoutOutputCollector collector;

    public SampledHashTagsSpout(String[] hashTags, int sampleCount) {
        super();
        this.hashTags = hashTags;
        this.sampleCount = sampleCount;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        long currentTime = getCurrentTime();
        Set<String> sampledData = doSample(currentTime, sampleCount, hashTags);
        collector.emit(new Values(sampledData));
        Utils.sleep(Constants.SAMPLED_DATA_EMIT_FREQUENCY);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SAMPLED_HASHTAGS_FIELD));
    }
}
