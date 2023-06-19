package unipi.cloudcomputing.randompick;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

public class RandomPickReducer extends Reducer<NullWritable, Sample, LongWritable, Text> {
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = context.getConfiguration().getInt("k", 0);
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Sample> values, Context context) throws IOException, InterruptedException {
        // Sample the first k samples
        SortedMap<Long, String> firstKSamples = RandomPickCombiner.sample(values, k);

        // Finally we emit the first k samples on this node
        for (Map.Entry<Long, String> entry : firstKSamples.entrySet()) {
            Long randomKey = entry.getKey();
            String sample = entry.getValue();
            context.write(new LongWritable(randomKey), new Text(sample));
        }
    }
}
