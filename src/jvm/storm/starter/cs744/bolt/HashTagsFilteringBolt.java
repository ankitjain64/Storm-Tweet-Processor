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
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.*;

/**
 * Filters Incoming Tweets based on hashTags set
 */
public class HashTagsFilteringBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Set<String> hashTags;
    private List<Status> tweets;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.hashTags = null;
        this.tweets = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        if (Constants.NUMBER_FILTERING_BOLT.equals(input.getSourceComponent())) {
            Status tweet = (Status) input.getValue(0);
            if (hashTags == null) {
                tweets.add(tweet);
            } else {
                emitOrIgnoreStatus(tweet);
            }
        } else {
            assert Constants.SAMPLED_HASHTAGS_SPOUT.equals(input.getSourceComponent());
            //noinspection unchecked
            hashTags = (Set<String>) input.getValue(0);
            if (!tweets.isEmpty()) {
                for (Status status : tweets) {
                    emitOrIgnoreStatus(status);
                }
                tweets = new ArrayList<>();
            }
        }
    }

    private void emitOrIgnoreStatus(Status tweet) {
        HashtagEntity[] hashtagEntities = tweet.getHashtagEntities();
        for (HashtagEntity hashtagEntity : hashtagEntities) {
            String hashTagText = hashtagEntity.getText();
            if (hashTags.contains(hashTagText)) {
                collector.emit(new Values(Utils.getSanitizedStringValue(tweet)));
                break;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.TWEET_TEXT_FIELD));
    }
}
