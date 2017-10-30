package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.starter.cs744.bolt.*;
import storm.starter.cs744.spout.ExtendedTwitterSpout;
import storm.starter.cs744.spout.NumbersSpout;
import storm.starter.cs744.spout.SampledHashTagsSpout;
import storm.starter.cs744.util.Constants;

import static storm.starter.cs744.util.Constants.*;
import static storm.starter.cs744.util.Utils.*;

public class CS744Assignment2_B2 {

    public static void main(String[] args) {
        int i = 0;
        String consumerKey = args[i++];
        String consumerSecret = args[i++];
        String accessToken = args[i++];
        String accessTokenSecret = args[i++];
        boolean isClusterMode = isClusterMode(args[i++]);
        String outputPathPrefix = args[i++];
        String[] keyWordsArr = extractCsvWords(args[i++]);
        String[] hashTags = extractCsvWords(args[i++]);
        String[] stopWordsArr = extractCsvWords(args[i++]);
        //noinspection UnusedAssignment
        Integer[] numbersArr = extractCsvNumbers(args[i++]);
        TopologyBuilder builder = new TopologyBuilder();

        //Instantiate various spouts and bolts
        ExtendedTwitterSpout twitterSampleSpout = new ExtendedTwitterSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keyWordsArr);
        HdfsBolt popularWordBolt = getHdfsBolt(outputPathPrefix + "/B_popularWords");
        HdfsBolt tweetsBolt = getHdfsBolt(outputPathPrefix + "/B_tweets_data");
        //noinspection ConstantConditions
        int sampleCount = hashTags.length - 5;
        if (sampleCount <= 0) {
            sampleCount = hashTags.length;
        }
        SampledHashTagsSpout sampledHashTagsSpout = new SampledHashTagsSpout(hashTags, sampleCount);
        NumbersSpout numberSpout = new NumbersSpout(numbersArr);
        StopWordsFilteringBolt stopWordBolt = new StopWordsFilteringBolt(stopWordsArr);

        buildTopology(builder, twitterSampleSpout, popularWordBolt,
                tweetsBolt, sampledHashTagsSpout, numberSpout, stopWordBolt, outputPathPrefix);
        Config config = new Config();
        config.setStatsSampleRate(1.0);
        int timeToRunTopologyInSeconds = 600;
        if (isClusterMode) {
            try {
                config.setNumWorkers(20);
                config.setMaxSpoutPending(5000);
                StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_TWO_NAME, config, builder.createTopology());
            } catch (TException e) {
                e.printStackTrace();
            }
        } else {
            try {
                config.setDebug(true);
                StormRunner.runTopologyLocally(builder.createTopology(), TOPOLOGY_TWO_NAME, config, timeToRunTopologyInSeconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    private static void buildTopology(TopologyBuilder builder, ExtendedTwitterSpout twitterSpout, HdfsBolt popularWordBolt, HdfsBolt tweetsBolt, SampledHashTagsSpout sampledHashTagsSpout, NumbersSpout numberSpout, StopWordsFilteringBolt stopWordBolt, String outputPathPrefix) {
        builder.setSpout(TWITTER_INPUT_SPOUT, twitterSpout);
        builder.setSpout(SAMPLED_HASHTAGS_SPOUT, sampledHashTagsSpout, 1);
        builder.setSpout(NUMBER_SPOUT, numberSpout, 1);
        builder.setBolt(NUMBER_FILTERING_BOLT, new FriendFilteringBolt()).allGrouping(NUMBER_SPOUT).shuffleGrouping(TWITTER_INPUT_SPOUT);
        builder.setBolt(HASHTAGS_FILTERING_BOLT, new HashTagsFilteringBolt()).allGrouping(SAMPLED_HASHTAGS_SPOUT).shuffleGrouping(NUMBER_FILTERING_BOLT);
        builder.setBolt(STOPWORDS_FILTERING_BOLT, stopWordBolt).shuffleGrouping(HASHTAGS_FILTERING_BOLT);
        Fields wordFields = new Fields(Constants.TWEET_WORD_FIELD);
        builder.setBolt(WORD_COUNT_BOLT, new WordCountBolt()).fieldsGrouping(STOPWORDS_FILTERING_BOLT, wordFields);
        builder.setBolt(POPULAR_WORDS_BOLT, new PopularWordsBolt(), 1).globalGrouping(WORD_COUNT_BOLT);
        builder.setBolt(HDFS_OUTPUT_BOLT + "_POPULAR", popularWordBolt, 1).globalGrouping(POPULAR_WORDS_BOLT);
        builder.setBolt(HDFS_OUTPUT_BOLT + "_TWEETS", tweetsBolt).shuffleGrouping(HASHTAGS_FILTERING_BOLT);
    }
}
