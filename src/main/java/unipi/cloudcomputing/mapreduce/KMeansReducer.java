package unipi.cloudcomputing.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import unipi.cloudcomputing.geometry.Point;
import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        // Sum points
        int cardinality = 1;
        Point sum = new Point(values.iterator().next());
        for(; values.iterator().hasNext(); cardinality++)
            sum.add(values.iterator().next());

        // Make average
        sum.scalarFactor(1d/(double)(cardinality));

        context.write(new Text(key.toString()), new Text(sum.toString()));
    }
}
