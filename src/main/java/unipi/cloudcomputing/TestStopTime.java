package unipi.cloudcomputing;

import org.apache.hadoop.conf.Configuration;
import unipi.cloudcomputing.geometry.Point;

import java.io.IOException;

public class TestStopTime {
    public static void main(String[] argv) throws IOException {
        Configuration conf = new Configuration();
        long startTime = System.currentTimeMillis();

        Point[] res = KMeansMapReduce.readCentroids(conf, 16, "/user/hadoop/project/output2000000_d6_run1/it1");

        for(Point p: res)
            System.out.println(p);

        long stopTime = System.currentTimeMillis();
        System.out.println("Ò impiegato " + (stopTime-startTime) + "ms per eseguire lettura da HDFS");
    }
}
