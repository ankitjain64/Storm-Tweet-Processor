package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import storm.starter.cs744.bolt.*;
import storm.starter.cs744.spout.ExtendedTwitterSpout;
import storm.starter.cs744.spout.NumbersSpout;
import storm.starter.cs744.spout.SampledHashTagsSpout;
import storm.starter.cs744.util.Constants;

import java.util.Map;

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
        SampledHashTagsSpout sampledHashTagsSpout = new SampledHashTagsSpout(hashTags, 30);
        NumbersSpout numberSpout = new NumbersSpout(numbersArr);
        StopWordsFilteringBolt stopWordBolt = new StopWordsFilteringBolt(stopWordsArr);

        buildTopology(builder, twitterSampleSpout, popularWordBolt,
                tweetsBolt, sampledHashTagsSpout, numberSpout, stopWordBolt, outputPathPrefix);
        Config config = new Config();
        int timeToRunTopologyInSeconds = 600;
        int timeToRunTopologyInMillis = timeToRunTopologyInSeconds * 1000;
        if (isClusterMode) {
            try {
                config.setNumWorkers(20);
                config.setMaxSpoutPending(5000);
                StormRunner.runTopologyRemotely(builder.createTopology(), TOPOLOGY_TWO_NAME, config);
                Thread.sleep(timeToRunTopologyInMillis);
                Map stormConfigMap = Utils.readStormConfig();
                Nimbus.Client nimbusClient = NimbusClient.getConfiguredClient(stormConfigMap).getClient();
                nimbusClient.killTopology(TOPOLOGY_TWO_NAME);
            } catch (InterruptedException | TException e) {
                e.printStackTrace();
            }
        } else {
            try {
                StormRunner.runTopologyLocally(builder.createTopology(), TOPOLOGY_TWO_NAME, config, timeToRunTopologyInSeconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    private static void buildTopology(TopologyBuilder builder, ExtendedTwitterSpout twitterSpout, HdfsBolt popularWordBolt, HdfsBolt tweetsBolt, SampledHashTagsSpout sampledHashTagsSpout, NumbersSpout numberSpout, StopWordsFilteringBolt stopWordBolt, String outputPathPrefix) {
        builder.setSpout(TWITTER_INPUT_SPOUT, twitterSpout);
        builder.setSpout(SAMPLED_HASHTAGS_SPOUT, sampledHashTagsSpout);
        builder.setSpout(NUMBER_SPOUT, numberSpout);
        builder.setBolt(NUMBER_FILTERING_BOLT, new FriendFilteringBolt()).allGrouping(NUMBER_SPOUT).shuffleGrouping(TWITTER_INPUT_SPOUT);
        builder.setBolt(HASHTAGS_FILTERING_BOLT, new HashTagsFilteringBolt()).allGrouping(SAMPLED_HASHTAGS_SPOUT).shuffleGrouping(NUMBER_FILTERING_BOLT);
        builder.setBolt(STOPWORDS_FILTERING_BOLT, stopWordBolt).shuffleGrouping(HASHTAGS_FILTERING_BOLT);
        Fields wordFields = new Fields(Constants.TWEET_WORD_FIELD);
        builder.setBolt(WORD_COUNT_BOLT, new WordCountBolt()).fieldsGrouping(STOPWORDS_FILTERING_BOLT, wordFields);
        builder.setBolt(POPULAR_WORDS_BOLT, new PopularWordsBolt()).globalGrouping(WORD_COUNT_BOLT);
        builder.setBolt(HDFS_OUTPUT_BOLT + "_POPULAR", popularWordBolt).globalGrouping(POPULAR_WORDS_BOLT);
        builder.setBolt(HDFS_OUTPUT_BOLT + "_TWEETS", tweetsBolt).globalGrouping(HASHTAGS_FILTERING_BOLT);
        builder.setBolt("test", getHdfsBolt(outputPathPrefix + "/tweet_test")).shuffleGrouping(TWITTER_INPUT_SPOUT);
    }
}
