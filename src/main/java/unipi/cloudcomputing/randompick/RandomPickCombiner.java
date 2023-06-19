package unipi.cloudcomputing.randompick;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RandomPickCombiner extends Reducer<NullWritable, Sample, NullWritable, Sample> {
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = context.getConfiguration().getInt("k", 0);
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Sample> values, Context context) throws IOException, InterruptedException {
        SortedMap<Long, String> firstKSamples = sample(values, k);

        // Finally we emit the first k samples on this node
        for (Map.Entry<Long, String> entry : firstKSamples.entrySet()) {
            Sample sample = new Sample();
            sample.sample = entry.getValue();
            sample.randomId = entry.getKey();

            context.write(key, sample);
        }
    }

    public static SortedMap<Long, String> sample(Iterable<Sample> values, int k) {
        SortedMap<Long, String> firstKSamples = new TreeMap<>();

        for (Sample sample : values) {
            // We don't even try to insert if the key is too big as it would be
            // removed at the next step otherwise.
            if (firstKSamples.size() > k && sample.randomId > firstKSamples.lastKey())
                continue;

            // Insert sample into data struct
            // O(log n)
            firstKSamples.put(sample.randomId, sample.sample);

            // If we have more than k samples, we then remove the bigger one
            if (firstKSamples.size() > k)
                // O(log n)
                firstKSamples.remove(firstKSamples.lastKey());
        }

        return firstKSamples;
    }
}
