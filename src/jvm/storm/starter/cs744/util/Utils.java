package storm.starter.cs744.util;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.tuple.Tuple;
import twitter4j.Status;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static java.lang.Boolean.TRUE;
import static storm.starter.cs744.util.Constants.DELIMETER;

public class Utils {

    public static HdfsBolt getHdfsBolt(String outputPath) {
        DelimitedRecordFormat delimitedRecordFormat = new
                DelimitedRecordFormat().withFieldDelimiter(DELIMETER);
        CountSyncPolicy countSyncPolicy = new CountSyncPolicy(1000);
        FileSizeRotationPolicy fileSizeRotationPolicy = new FileSizeRotationPolicy(5f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(outputPath);
        return new HdfsBolt()
                .withFsUrl(Constants.HDFS_URI)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(delimitedRecordFormat)
                .withRotationPolicy(fileSizeRotationPolicy)
                .withSyncPolicy(countSyncPolicy);
    }


    public static String[] extractKeyWords(String[] args, int i) {
        List<String> keyWordList = new ArrayList<>();
        keyWordList.addAll(Arrays.asList(args).subList(i, args.length));
        return keyWordList.toArray(new String[keyWordList.size()]);
    }

    public static Integer[] extractCsvNumbers(String csvFilePath) {
        String[] strings = extractCsvWords(csvFilePath);
        Integer[] rv = new Integer[strings.length];
        int count = 0;
        for (String numberStr : strings) {
            rv[count++] = Integer.parseInt(numberStr);
        }
        return rv;
    }

    public static String[] extractCsvWords(String csvFilePath) {
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(csvFilePath);
            br = new BufferedReader(fr);
            String s = br.readLine();
            if (s == null || s.length() == 0) {
                System.out.println("No line found at csvPath: " + csvFilePath);
                System.exit(-1);
            }
            return s.split(",");
        } catch (IOException e) {
            System.out.println("Exception occurred while reading file: " +
                    csvFilePath + "Exception as string: " + e.toString());
            System.exit(-1);
        } finally {
            safeCloseReaders(br, fr);
        }
        return null;
    }

    private static void safeCloseReaders(BufferedReader br, FileReader fr) {
        if (br != null) {
            try {
                br.close();
                if (fr != null) {
                    fr.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getSanitizedStringValue(Status tweet) {
        return tweet.getText().replace('\n', ' ');
    }

    public static boolean isClusterMode(String clusterModeStr) {
        return TRUE.toString().equals(clusterModeStr);
    }

    public static long getCurrentTime() {
        return new Date().getTime();
    }

    public static <T> Set<T> doSample(long seed, int sampleCount, T[] toBeSampled) {
        if (toBeSampled == null) {
            return null;
        }
        Set<T> rv = new HashSet<>();
        Random random = new Random(seed);
        Set<Integer> indexSet = new HashSet<>();
        while (indexSet.size() != sampleCount) {
            indexSet.add(random.nextInt(toBeSampled.length));
        }
        for (Integer index : indexSet) {
            rv.add(toBeSampled[index]);
        }
        return rv;
    }


    public static <E> boolean isEmpty(Collection<E> collection) {
        return collection == null || collection.isEmpty();
    }


    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(org.apache.storm.Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(org.apache.storm.Constants.SYSTEM_TICK_STREAM_ID);
    }
}
