package storm.starter.cs744.spout;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static storm.starter.cs744.util.Constants.SAMPLED_DATA_EMIT_FREQUENCY;
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
    private Long lastEmitTime;

    public SampledHashTagsSpout(String[] hashTags, int sampleCount) {
        super();
        int i = 0;
        Set<String> hashTagsSet = new HashSet<>();
        //Remove Hash
        for (String hashTag : hashTags) {
            int i1 = hashTag.indexOf('#');
            if (i1 >= 0) {
                hashTagsSet.add(hashTag.substring(i1 + 1).trim().toLowerCase());
            } else {
                hashTagsSet.add(hashTag.trim().toLowerCase());
            }
            i++;
        }
        this.hashTags = new String[hashTagsSet.size()];
        i = 0;
        for (String hashTag : hashTagsSet) {
            this.hashTags[i] = hashTag;
            i++;
        }
        this.sampleCount = sampleCount;
        this.lastEmitTime = null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        long currentTime = getCurrentTime();
        if (this.lastEmitTime == null) {
            doEmit(currentTime);
        } else {
            long x = lastEmitTime + SAMPLED_DATA_EMIT_FREQUENCY;
            if (Long.compare(currentTime, x) > 0) {
                doEmit(currentTime);
            } else {
                Utils.sleep(50);
            }
        }
    }

    private void doEmit(long currentTime) {
        List<String> sampledData = doSample(currentTime, sampleCount, hashTags);
        collector.emit(new Values(sampledData));
        this.lastEmitTime = currentTime;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SAMPLED_HASHTAGS_FIELD));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }
}
