package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.TopologyBuilder;
import storm.starter.cs744.bolt.CountLimiterBolt;
import storm.starter.cs744.spout.ExtendedTwitterSpout;

import static storm.starter.cs744.util.Constants.*;
import static storm.starter.cs744.util.Utils.*;

public class CS744Assignment2_B1 {
    public static void main(String[] args) {
        int i = 0;
        String consumerKey = args[i++];
        String consumerSecret = args[i++];
        String accessToken = args[i++];
        String accessTokenSecret = args[i++];
        boolean isClusterMode = isClusterMode(args[i++]);
        String outputPath = args[i++];
        int maxTweets = Integer.parseInt(args[i++]);
        //noinspection UnusedAssignment
        String[] keyWordsArray = extractCsvWords(args[i++]);
        ExtendedTwitterSpout twitterSampleSpout = new ExtendedTwitterSpout(consumerKey, consumerSecret, accessToken,
                accessTokenSecret, keyWordsArray);
        TopologyBuilder builder = new TopologyBuilder();

        Config config = new Config();

        HdfsBolt hdfsBolt = getHdfsBolt(outputPath);
        builder.setSpout(TWITTER_INPUT_SPOUT, twitterSampleSpout);
        CountLimiterBolt bolt = new CountLimiterBolt(maxTweets, isClusterMode);
        builder.setBolt(TWEET_COUNT_BOLT, bolt).globalGrouping(TWITTER_INPUT_SPOUT);
        if (isClusterMode) {
            builder.setBolt(HDFS_OUTPUT_BOLT, hdfsBolt).shuffleGrouping(TWEET_COUNT_BOLT);
            config.setNumWorkers(20);
            config.setMaxSpoutPending(5000);
            try {
                StormRunner.runTopologyRemotely(builder.createTopology(), TOPOLOGY_ONE_NAME, config);
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            builder.setBolt(HDFS_OUTPUT_BOLT, hdfsBolt).globalGrouping(TWEET_COUNT_BOLT);
            config.setMaxTaskParallelism(3);
            try {
                StormRunner.runTopologyLocally(builder.createTopology(),
                        TOPOLOGY_ONE_NAME, config, 600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
