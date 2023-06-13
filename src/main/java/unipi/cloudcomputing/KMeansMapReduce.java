package unipi.cloudcomputing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.GenericOptionsParser;

import unipi.cloudcomputing.geometry.Point;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class KMeansMapReduce {
    private static boolean stopCriterion(Point[] oldC, Point[] newC, int normT, float threshold) {
        for(int i = 0; i < oldC.length; i++)
            if(Point.distance(oldC[i],newC[i], normT) > threshold)
                return false;

        return true;
    }

    private static Point[] centroidsInit(Configuration conf, String pathString, int k, int dataSetSize)
            throws IOException {
        Point[] points = new Point[k];

        List<Integer> positions = new ArrayList<Integer>();
        Random random = new Random();
        int pos;
        while(positions.size() < k) {
            pos = random.nextInt(dataSetSize);
            if(!positions.contains(pos)) {
                positions.add(pos);
            }
        }
        Collections.sort(positions);

        //File reading utils
        Path path = new Path(pathString);
        FileSystem hdfs = FileSystem.get(conf);
        FSDataInputStream in = hdfs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        //Get centroids from the file
        int row = 0;
        int i = 0;
        int position;
        while(i < positions.size()) {
            position = positions.get(i);
            String point = br.readLine();
            if(row == position) {
                points[i] = Point.fromString(point.split(","));
                i++;
            }
            row++;
        }
        br.close();

        return points;
    }

    private static Point[] readCentroids(Configuration conf, int k, String pathString)
            throws IOException, FileNotFoundException {
        Point[] points = new Point[k];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));

        for (FileStatus fileStatus : status) {
            //Read the centroids from the hdfs
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(fileStatus.getPath())));
                String[] keyValueSplit = br.readLine().split("\t"); //Split line in K,V
                int centroidId = Integer.parseInt(keyValueSplit[0]);
                String[] point = keyValueSplit[1].split(",");
                points[centroidId] = Point.fromString(point);
                br.close();
            }
        }
        //Delete temp directory
        hdfs.delete(new Path(pathString), true);

        return points;
    }

    private static void finalize(Configuration conf, Point[] centroids, String output) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(output + "/centroids.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        //Write the result in a unique file
        for (Point centroid : centroids) {
            br.write(centroid.toString());
            br.newLine();
        }

        br.close();
        hdfs.close();
    }

    public static void main( String[] args ) throws IOException {
        Configuration conf = new Configuration();
        final String[] genericArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        final String INPUT = genericArgs[0];
        final String OUTPUT = genericArgs[1] + "/tmp";
        final int DATASET_SIZE = 10; // @TODO XML
        final int DISTANCE = 2;
        final int K = 3;
        final double THRESHOLD = 0.0001;
        final int MAX_ITERATIONS = 30;

        Point[] newCentroids = new Point[K];
        Point[] oldCentroids = new Point[K];


    }
}
