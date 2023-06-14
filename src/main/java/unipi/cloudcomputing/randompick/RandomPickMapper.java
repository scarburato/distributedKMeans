package unipi.cloudcomputing.randompick;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Random;

/**
 * This mapper is used to sample random points from the dataset. It'll be used
 * in k-means to draw the k random centroinds in the setup step
 */
public class RandomPickMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>{
    private Random randomGenerator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //k = Integer.parseInt(context.getConfiguration().get("k"));
        // @TODO pass seed as parameter
        randomGenerator = new Random(0xcafebabeL);
    }

    /**
     * Basically we add a random label to each data tuple.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(randomGenerator.nextLong()), value);
    }
}
