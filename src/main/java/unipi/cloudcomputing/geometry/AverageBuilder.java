package unipi.cloudcomputing.geometry;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class helps to build an average point from several
 * points. To do so it stores the points' sum and their
 * cardinality. Users can call computeAverage() to compute
 * their average.
 */
public class AverageBuilder implements Writable {
    Point sum;
    long cardinality;

    public AverageBuilder(Point addendum) {
        sum = new Point(addendum);
        cardinality = 1;
    }

    public AverageBuilder() {
        sum = new Point();
        cardinality = 0;
    };

    public void addToComputation(final Point pt) {
        sum.add(pt);
        cardinality ++;
    }

    public void addToComputation(final AverageBuilder bl) {
        sum.add(bl.sum);
        cardinality += bl.cardinality;
    }

    public Point computeAverage() {
        Point avg = new Point(sum);
        avg.scalarFactor(1d/(double) cardinality);
        return avg;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        sum.write(dataOutput);
        dataOutput.writeLong(cardinality);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sum.readFields(dataInput);
        cardinality = dataInput.readLong();
    }
}
