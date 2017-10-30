package storm.starter.cs744.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;

public class NativeFileWriterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 12345567L;
    private static String fileToWrite;

    public NativeFileWriterBolt(String fileToWrite) {
        this.fileToWrite = fileToWrite;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple);
        writeStringToFile(fileToWrite, ((String) tuple.getValue(0)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    private void writeStringToFile(String filePath, String outputString) {
        try {
            FileWriter fw = new FileWriter(filePath, true);
            fw.write(outputString + "\n");
            fw.close();
        } catch (IOException e) {
            System.err.println("IOException" + e.getLocalizedMessage());
        }
    }

}
