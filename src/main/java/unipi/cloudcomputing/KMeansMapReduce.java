package unipi.cloudcomputing;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import unipi.cloudcomputing.geometry.AverageBuilder;
import unipi.cloudcomputing.geometry.Point;
import unipi.cloudcomputing.mapreduce.KMeansCombiner;
import unipi.cloudcomputing.mapreduce.KMeansMapper;
import unipi.cloudcomputing.mapreduce.KMeansReducer;
import unipi.cloudcomputing.randompick.*;

import java.io.*;
import java.util.Arrays;


public class KMeansMapReduce {
    private static boolean stopCriterion(Point[] oldC, Point[] newC, int normT, double threshold) {
        for(int i = 0; i < oldC.length; i++)
            if(Point.distance(oldC[i],newC[i], normT) > threshold)
                return false;

        return true;
    }

    /**
     * Starts a Hadoop job and randomly picks k points as centroids
     */
    private static Point[] centroidsInit(Configuration conf, FileSystem hdfs, String pathString, String outString, int k)
            throws IOException, InterruptedException, ClassNotFoundException {
        return RandomPickManager.pick(pathString, outString, k, hdfs)
                .map(s -> s.split("\t")[1])
                .map(Point::fromString)
                .toArray(Point[]::new);
    }

    /**
     * Reads the centroids from file
     */
    private static Point[] readCentroids(Configuration conf, int k, String pathString)
            throws IOException, FileNotFoundException {
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] nodes = hdfs.listStatus(new Path(pathString), new GlobFilter("part-r-*"));

        if(nodes.length == 0)
            throw new RuntimeException("Unable to find initial centroids' files in " + pathString);

        Point[] centroids = new Point[k];
        for(FileStatus node : nodes) {
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(node.getPath())));
            String[] data = br.readLine().split("\t");

            centroids[Integer.parseInt(data[0])] = Point.fromString(data[1]);
        }

        return centroids;
    }

    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        final String[] genericArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // cmd options parser
        final HelpFormatter formatter = new HelpFormatter();
        final CommandLineParser parser = new GnuParser();
        final CommandLine cmd;
        final KMeansOptions options = new KMeansOptions();
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("-i INPUT -o OUTPUT -d DIMENSIONALITY [-t THRESHOLD]", options);
            System.exit(1);
            return;
        }

        final String INPUT = cmd.getOptionValue("input");
        final String OUTPUT = cmd.getOptionValue("output") /*+ "/tmp"*/;
        final int DATASET_SIZE = Integer.parseInt(cmd.getOptionValue("dimensionality"));
        final int DISTANCE = Integer.parseInt(
                cmd.getOptionValue("norm") != null ?
                        cmd.getOptionValue("norm") : "2");
        final int K = Integer.parseInt(
                cmd.getOptionValue("clusters") != null ?
                        cmd.getOptionValue("clusters") : "3");
        final double THRESHOLD = Double.parseDouble(
                cmd.getOptionValue("threshold") != null ?
                        cmd.getOptionValue("threshold") : "0.0001");
        final int MAX_ITERATIONS = Integer.parseInt(
                cmd.getOptionValue("maxiterations") != null ?
                        cmd.getOptionValue("maxiterations") : "30");

        conf.setInt("k", K);
        conf.setInt("distance", DISTANCE);

        final FileSystem hdfs = FileSystem.get(conf);

        int iterations = 0;

        Point[] newCentroids = centroidsInit(conf, hdfs, INPUT, OUTPUT + "/centroids.init", K);
                /*new Point[]{
                new Point(new double[]{-90.288,-193.43,-145.01,79.46,160.68,-93.544,205.47,-175.79,151.11,117.69,-133.95,52.313}),
                new Point(new double[]{9.4601,66.414,89.96,43.682,163.31,-1.8947,68.156,-167.06,-18.935,111.58,149.43,-39.676}),
                new Point(new double[]{-59.114,-192.03,-144.55,68.621,131.91,-80.578,199.91,-168.95,138.94,117.16,-147.6,54.316})
        };*/
        Point[] oldCentroids;

        long time_start = System.currentTimeMillis();
        do {
            iterations ++;

            Job job = Job.getInstance(conf, "iteration_" + iterations);

            // Save centroids in conf
            for(int i = 0; i < K; i++) {
                job.getConfiguration().unset("centroid." + i);
                job.getConfiguration().set("centroid." + i, newCentroids[i].toString());
            }

            // Set adapters
            job.setJarByClass(KMeansMapReduce.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);

            job.setNumReduceTasks(K);

            // Set input 'n output 'n stuff
            FileInputFormat.addInputPath(job, new Path(INPUT));
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT + "/it" + iterations));

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(AverageBuilder.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputValueClass(TextOutputFormat.class);

            // Boot-up the job!
            boolean success = job.waitForCompletion(true);
            if(!success) {
                System.err.println(job.getJobName() + " has failed");
                System.exit(0xff);
            }

            // new centroids incoming!
            oldCentroids = newCentroids;
            newCentroids = readCentroids(conf, K,OUTPUT + "/it" + iterations);
        } while (iterations <= MAX_ITERATIONS && !stopCriterion(oldCentroids, newCentroids, DISTANCE, THRESHOLD));

        long time_stop = System.currentTimeMillis();

        System.out.println("Ò fatto iterazioni in numero " + iterations);
        System.out.println("Ò lavorato per millisecondi " + (time_stop-time_start));

        FSDataOutputStream dos = hdfs.create(new Path(OUTPUT + "/centroids"), true);

        Arrays.stream(newCentroids).map(Point::toString).forEach(s -> {
            try {
                dos.writeBytes(s);
                dos.write('\n');
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        dos.flush();
        dos.close();

        hdfs.close();
    }
}
