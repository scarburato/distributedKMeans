package unipi.cloudcomputing.randompick;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Test {
    public static void main(String[] argv) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.setInt("k", Integer.parseInt(argv[0]));

        Job job = Job.getInstance(conf, "randomPickerTest");

        job.setJarByClass(Test.class);

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
        FileInputFormat.addInputPath(job, new Path(argv[1]));
        FileOutputFormat.setOutputPath(job, new Path(argv[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

        boolean success = job.waitForCompletion(true);
        System.out.println("Success: " + (success ? "YES" : "NO"));
    }
}
