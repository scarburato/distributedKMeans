package unipi.cloudcomputing.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import unipi.cloudcomputing.geometry.AverageBuilder;
import unipi.cloudcomputing.geometry.Point;

import java.io.IOException;
import java.util.Arrays;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, AverageBuilder> {
    private Point[] centroids;
    private int p;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int k = context.getConfiguration().getInt("k", -1);
        p = context.getConfiguration().getInt("distance", 2);

        // Load centroids
        centroids = new Point[k];
        for(int i = 0; i < k; i++)
            centroids[i] = Point.fromString(context.getConfiguration().get("centroid." + i));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Load datum's coordinates from string (from file)
        Point datum = Point.fromString(value.toString().split(","));

        double minDist = Double.POSITIVE_INFINITY;
        int closestCentroid = -1;

        // Find closest centroid to datum
        // tetha(k)
        for(int i = 0; i < centroids.length; i++) {
            // tetha(d)
            double distance = Point.distance(centroids[i], datum, p);

            if(distance >= minDist)
                continue;

            closestCentroid = i;
            minDist = distance;
        }

        // Output closest centroid
        IntWritable t = new IntWritable();
        t.set(closestCentroid);
        context.write(t, new AverageBuilder(datum));
    }
}
