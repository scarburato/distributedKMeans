package unipi.cloudcomputing.randompick;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RandomPickReducer extends Reducer<ShortWritable, Sample, LongWritable, Text> {
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = context.getConfiguration().getInt("k", 0);
    }

    @Override
    protected void reduce(ShortWritable key, Iterable<Sample> values, Context context) throws IOException, InterruptedException {
        // Sample the first k samples
        SortedMap<Long, String> firstKSamples = RandomPickCombiner.sample(key, values, k);

        // Finally we emit the first k samples on this node
        for (Map.Entry<Long, String> entry : firstKSamples.entrySet()) {
            Long randomKey = entry.getKey();
            String sample = entry.getValue();
            context.write(new LongWritable(randomKey), new Text(sample));
        }
    }
}
