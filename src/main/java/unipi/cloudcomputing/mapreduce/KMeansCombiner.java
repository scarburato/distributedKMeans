package unipi.cloudcomputing.mapreduce;

import org.apache.hadoop.io.IntWritable;
import unipi.cloudcomputing.geometry.Point;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

    @Override
    protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        Point sum = new Point(values.iterator().next());
        values.forEach(sum::add);

        context.write(key, sum);
    }
}
