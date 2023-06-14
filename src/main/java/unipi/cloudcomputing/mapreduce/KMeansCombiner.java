package unipi.cloudcomputing.mapreduce;

import org.apache.hadoop.io.IntWritable;
import unipi.cloudcomputing.geometry.AverageBuilder;
import unipi.cloudcomputing.geometry.Point;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, AverageBuilder> {

    @Override
    protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        AverageBuilder sum = new AverageBuilder();

        // Compute partial sum for the sub-set of points of cluster `key`
        values.forEach(sum::addToComputation);

        // Output
        context.write(key, sum);
    }
}
