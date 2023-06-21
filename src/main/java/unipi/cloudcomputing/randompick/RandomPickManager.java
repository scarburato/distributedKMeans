package unipi.cloudcomputing.randompick;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Stream;

public class RandomPickManager {
    public static void main(String[] argv) throws IOException, InterruptedException, ClassNotFoundException {
        Stream<String> samples = pick(argv[1], argv[2], Integer.parseUnsignedInt(argv[0]), null);
        System.out.println("Success: " + (samples != null ? "YES" : "NO"));
    }

    /**
     * Picks `samples` random samples
     */
    public static Stream<String> pick(String inPath, String outPath, int samples, FileSystem hdfs) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.setInt("k", samples);

        Job job = Job.getInstance(conf, "randomPickerTest");

        job.setJarByClass(RandomPickManager.class);

        // set mapper/reducer
        job.setMapperClass(RandomPickMapper.class);
        job.setCombinerClass(RandomPickCombiner.class);
        job.setReducerClass(RandomPickReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Sample.class);

        // define reducer's output key-value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

        boolean success = job.waitForCompletion(true);
        if(!success)
            return null;

        if(hdfs == null)
            return Stream.empty();

        FileStatus[] nodes = hdfs.listStatus(new Path(outPath), new GlobFilter("part-r-*"));

        if(nodes.length == 0)
            throw new RuntimeException("Unable to find initial centroids' files in " + outPath);

        return new BufferedReader(new InputStreamReader(hdfs.open(nodes[0].getPath()))).lines();
    }
}
