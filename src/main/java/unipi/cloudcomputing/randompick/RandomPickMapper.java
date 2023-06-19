package unipi.cloudcomputing.randompick;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

/**
 * This mapper is used to sample random points from the dataset. It'll be used
 * in k-means to draw the k random centroinds in the setup step
 */
public class RandomPickMapper extends Mapper<LongWritable, Text, NullWritable, Sample> {
    private Random randomGenerator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        randomGenerator = new Random(); // 0xcafebabeL
    }

    /**
     * Basically we add a random label to each data tuple.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Sample sample = new Sample();
        sample.randomId = randomGenerator.nextLong();
        sample.sample = value.toString();
        context.write(NullWritable.get(), sample);
    }
}
