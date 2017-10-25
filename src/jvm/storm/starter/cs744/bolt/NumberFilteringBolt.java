package storm.starter.cs744.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.starter.cs744.util.Constants;
import storm.starter.cs744.util.Utils;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static storm.starter.cs744.util.Constants.TWEET_FIELD;

public class NumberFilteringBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Integer sampledNumber;
    private List<Status> tweetStatusList;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.tweetStatusList = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals(Constants.TWITTER_INPUT_SPOUT)) {
            Status currentStatus = (Status) input.getValue(0);
            if (sampledNumber == null) {
                tweetStatusList.add(currentStatus);
            } else {
                emitOrIgnore(currentStatus);
            }
        } else {
            assert Constants.NUMBER_SPOUT.equals(input.getSourceComponent());
            //noinspection unchecked
            this.sampledNumber = (Integer) input.getValue(0);
            if (!Utils.isEmpty(tweetStatusList)) {
                for (Status status : tweetStatusList) {
                    emitOrIgnore(status);
                }
            }
            this.tweetStatusList = new ArrayList<>();
        }
    }

    private void emitOrIgnore(Status status) {
        if (status.getUser().getFriendsCount() < this.sampledNumber) {
            collector.emit(new Values(status));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TWEET_FIELD));
    }
}
