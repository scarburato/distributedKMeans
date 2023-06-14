package unipi.cloudcomputing.randompick;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RandomPickCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = Integer.parseInt(context.getConfiguration().get("k"));
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        SortedMap<Long, String> firstKSamples = new TreeMap<>();

        values.forEach(text -> {
            // We don't even try to insert if the key is too big as it would be
            // removed at the next step otherwise.
            if(key.get() > firstKSamples.lastKey())
                return;

            // Insert sample into data struct
            // O(log n)
            firstKSamples.put(key.get(), values.toString());

            // If we have more than k samples, we then remove the bigger one
            if(firstKSamples.size() > k)
                // O(log n)
                firstKSamples.remove(firstKSamples.lastKey());
        });

        // Finally we emit the first k samples on this node
        for (Map.Entry<Long, String> entry : firstKSamples.entrySet()) {
            Long randomKey = entry.getKey();
            String sample = entry.getValue();
            context.write(new LongWritable(randomKey), new Text(sample));
        }
    }
}
