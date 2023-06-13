package unipi.cloudcomputing.mapreduce;

import org.apache.hadoop.io.IntWritable;
import unipi.cloudcomputing.geometry.AverageBuilder;
import unipi.cloudcomputing.geometry.Point;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, AverageBuilder> {

    @Override
    protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
        AverageBuilder sum = new AverageBuilder(values.iterator().next());
        values.forEach(sum::addToComputation);

        context.write(key, sum);
    }
}
