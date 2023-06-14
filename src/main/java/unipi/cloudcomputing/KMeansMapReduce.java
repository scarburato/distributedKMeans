package unipi.cloudcomputing;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import unipi.cloudcomputing.randompick.RandomPickCombiner;
import unipi.cloudcomputing.randompick.RandomPickMapper;
import unipi.cloudcomputing.randompick.RandomPickReducer;

import java.io.*;


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
    private static Point[] centroidsInit(Configuration conf, String pathString, String outString, int k)
            throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "pick initial centroids");

        // Set adapters
        job.setJarByClass(KMeansMapReduce.class);
        job.setMapperClass(RandomPickMapper.class);
        job.setCombinerClass(RandomPickCombiner.class);
        job.setReducerClass(RandomPickReducer.class);

        // Just one reducer for this
        job.setNumReduceTasks(1);

        // Set input 'n output 'n stuff
        FileInputFormat.addInputPath(job, new Path(pathString));
        FileOutputFormat.setOutputPath(job, new Path(outString));

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);

        // Boot-up the job!
        boolean success = job.waitForCompletion(true);
        if(!success) {
            System.err.println(job.getJobName() + " has failed");
            System.exit(0xee);
        }

        // @TODO testing only
        System.exit(0);

        return readCentroids(conf, k, outString);
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
            formatter.printHelp("", options);
            System.exit(1);
            return;
        }

        final String INPUT = cmd.getOptionValue("input");
        final String OUTPUT = cmd.getOptionValue("output") + "/tmp";
        final int DATASET_SIZE = Integer.parseInt(cmd.getOptionValue("dimensionality"));
        final int DISTANCE = Integer.parseInt(
                cmd.getOptionValue("norm") != null ?
                        cmd.getOptionValue("norm") : "10");
        final int K = Integer.parseInt(
                cmd.getOptionValue("clusters") != null ?
                        cmd.getOptionValue("clusters") : "3");
        final double THRESHOLD = Double.parseDouble(
                cmd.getOptionValue("threshold") != null ?
                        cmd.getOptionValue("threshold") : "0.0001");
        final int MAX_ITERATIONS = Integer.parseInt(
                cmd.getOptionValue("maxiterations") != null ?
                        cmd.getOptionValue("maxiterations") : "10");

        int iterations = 0;

        Point[] newCentroids = centroidsInit(conf, INPUT, OUTPUT + "/centroids.init", K);
        Point[] oldCentroids;

        long time_start = System.currentTimeMillis();
        do {
            iterations ++;
            Job job = Job.getInstance(conf, "iteration_" + iterations);

            // Set adapters
            job.setJarByClass(KMeansMapReduce.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);

            job.setNumReduceTasks(K);

            // Set input 'n output 'n stuff
            FileInputFormat.addInputPath(job, new Path(INPUT));
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT));

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(AverageBuilder.class);
            job.setMapOutputValueClass(Point.class);

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
            newCentroids = readCentroids(conf, K, OUTPUT);

            // Save new centroids in conf
            for(int i = 0; i < K; i++) {
                conf.unset("centroid-" + i);
                conf.set("centroid-" + i, newCentroids[i].toString());
            }

        } while (iterations <= MAX_ITERATIONS && !stopCriterion(oldCentroids, newCentroids, DISTANCE, THRESHOLD));

        long time_stop = System.currentTimeMillis();

        // Clean-it-up
        finalize(conf, newCentroids, genericArgs[1]);

        System.out.println("Ò fatto iterazioni in numero " + iterations);
        System.out.println("Ò lavorato per millisecondi " + (time_stop-time_start));
    }
}
