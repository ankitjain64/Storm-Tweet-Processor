package storm.starter.cs744.bolt;

import org.apache.storm.LocalCluster;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import storm.starter.cs744.util.Constants;
import twitter4j.Status;

import java.util.Map;

import static storm.starter.cs744.util.Constants.TOPOLOGY_ONE_NAME;
import static storm.starter.cs744.util.Utils.getSanitizedStringValue;

/**
 * Stops the topology when maximum count has reached and emits sanitized text
 * of tweets in the stream
 */
public class CountLimiterBolt extends BaseRichBolt {
    private int maxCount;
    private transient LocalCluster localCluster;
    private boolean isClusterMode;
    private int currentCount;
    private OutputCollector outputCollector;

    public CountLimiterBolt(int maxCount) {
        this.maxCount = maxCount;
        this.isClusterMode = true;
        this.currentCount = 0;
    }

    public CountLimiterBolt(int maxCount, LocalCluster localCluster) {
        this.maxCount = maxCount;
        this.localCluster = localCluster;
        this.isClusterMode = false;
        this.currentCount = 0;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        this.currentCount++;
        Status inputStatus = (Status) tuple.getValue(0);
        Values outputTuple = new Values(getSanitizedStringValue(inputStatus));
        if (currentCount <= maxCount) {
            //to handle in local topology
            outputCollector.emit(outputTuple);
        }
        if (this.currentCount == this.maxCount) {
            if (isClusterMode) {
                //stop the topology
                Map stormConfigMap = Utils.readStormConfig();
                Nimbus.Client nimbusClient = NimbusClient.getConfiguredClient(stormConfigMap).getClient();
                try {
                    nimbusClient.killTopology(TOPOLOGY_ONE_NAME);
                } catch (TException e) {
                    e.printStackTrace();
                }
            } else {
                localCluster.killTopology(TOPOLOGY_ONE_NAME);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.TWEET_TEXT_FIELD));
    }
}
