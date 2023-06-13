package unipi.cloudcomputing.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import unipi.cloudcomputing.geometry.AverageBuilder;
import unipi.cloudcomputing.geometry.Point;
import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, AverageBuilder, Text, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<AverageBuilder> values, Context context) throws IOException, InterruptedException {
        AverageBuilder averageBuilderEnjoyer = values.iterator().next();
        values.forEach(averageBuilderEnjoyer::addToComputation);

        context.write(new Text(key.toString()), new Text(averageBuilderEnjoyer.computeAverage().toString()));
    }
}
