package unipi.cloudcomputing.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import unipi.cloudcomputing.geometry.Point;

import java.io.IOException;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
    private Point[] centroids;
    private int p;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int k = Integer.parseInt(context.getConfiguration().get("k"));
        p = Integer.parseInt(context.getConfiguration().get("distance"));

        centroids = new Point[k];

        // Init centroids
        for(int i = 0; i < k; i++) {
            // Parse 'n stuff
            String[] centroid_comps_str = context.getConfiguration().getStrings("centroid-"+i);
            double[] centroid_comps = new double[centroid_comps_str.length];

            for(int j = 0; j < centroid_comps.length; j++)
                centroid_comps[j] = Double.parseDouble(centroid_comps_str[j]);

            centroids[i] = new Point(centroid_comps);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Load centroid's data from strings
        String[] datum_comps_str = value.toString().split(",");
        double[] datum_comps = new double[datum_comps_str.length];

        for(int j = 0; j < datum_comps.length; j++)
            datum_comps[j] = Double.parseDouble(datum_comps_str[j]);

        Point datum = new Point(datum_comps);

        double minDist = Double.POSITIVE_INFINITY;
        int closestCentroid = -1;

        // Find closest centroid to datum
        for(int i = 0; i < centroids.length; i++) {
            double distance = Point.distance(centroids[i], datum, p);

            if(distance >= minDist)
                continue;

            closestCentroid = i;
            minDist = distance;
        }

        // Output closest centroid
        IntWritable t = new IntWritable();
        t.set(closestCentroid);
        context.write(t, datum);
    }
}
