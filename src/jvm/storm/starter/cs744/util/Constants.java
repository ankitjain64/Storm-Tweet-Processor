package storm.starter.cs744.util;

public class Constants {
    //Configs
    public static final String HDFS_URI = "hdfs://10.254.0.14:8020";
    public static final int WORD_COUNT_EMIT_FREQUENCY = 30;
    public static final int SAMPLED_DATA_EMIT_FREQUENCY = WORD_COUNT_EMIT_FREQUENCY * 1000;

    //TOPOLOGY NAME CONSTANT
    public static final String TOPOLOGY_ONE_NAME = "CS744Assignment2_B1";
    public static final String TOPOLOGY_TWO_NAME = "CS744Assignment2_B2";
    public static final String DELIMETER = "-:-";

    //BOLT_IDS
    public static final String NATIVE_FS_OUTPUT_BOLT = "NATIVE_FS_OUTPUT_BOLT";
    public static final String HDFS_OUTPUT_BOLT = "HDFS_OUTPUT_BOLT";
    public static final String TWEET_COUNT_BOLT = "TWEET_COUNT_BOLT";
    public static final String NUMBER_FILTERING_BOLT = "NUMBER_FILTERING_BOLT";
    public static final String HASHTAGS_FILTERING_BOLT = "HASHTAGS_FILTERING_BOLT";
    public static final String STOPWORDS_FILTERING_BOLT = "STOPWORDS_FILTERING_BOLT";
    public static final String WORD_COUNT_BOLT = "WC_BOLT";
    public static final String POPULAR_WORDS_BOLT = "POPULAR_WORDS_BOLT";

    //SPOUT_IDS
    public static final String TWITTER_INPUT_SPOUT = "TWITTER_INPUT_SPOUT";
    public static final String SAMPLED_HASHTAGS_SPOUT = "SAMPLED_HASHTAGS_SPOUT";
    public static final String NUMBER_SPOUT = "NUMBER_SPOUT";

    //Fields
    public static final String TS_FIELD = "ts";
    public static final String TWEET_FIELD = "tweet";
    public static final String SAMPLED_NUMBER_FIELD = "sampledNumber";
    public static final String TWEET_TEXT_FIELD = "tweetText";
    public static final String SAMPLED_HASHTAGS_FIELD = "sampledHashTags";
    public static final String TWEET_WORD_FIELD = "tweetWord";
    public static final String WORD_FREQUENCY_FIELD = "wordFrequency";
    public static final String POPULAR_WORDS_FIELD = "popularWords";

}
