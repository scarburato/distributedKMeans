package unipi.cloudcomputing.geometry;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Point implements Writable {
    private double[] components;
    private int dimension;

    public Point() {
        dimension = 0;
    }

    /**
     * Works kinda like a C++ copy constructor
     * @param c A point to copy
     */
    public Point(final Point c) {
        dimension = c.dimension;
        if(dimension > 0)
            components = Arrays.copyOf(c.components, c.dimension);
    }

    public Point(final double[] vals) {
        set(vals);
    }

    public void set(final double[] vals) {
        components = vals;
        dimension = components.length;
    }

    public void add(final Point addendum) {
        // If the object is un-initialized, just make a copy
        if (dimension == 0 && addendum.dimension > 0) {
            dimension = addendum.dimension;
            components = Arrays.copyOf(addendum.components, addendum.dimension);
            return;
        }

        if (addendum.dimension != dimension)
            throw new IllegalArgumentException("Addenda's dimensionalities must be the same!");

        for(int i = 0; i < dimension; i++)
            components[i] += addendum.components[i];
    }

    public void sub(final Point addendum) {
        // If the object is un-initialized, just make a copy
        if (dimension == 0 && addendum.dimension > 0) {
            dimension = addendum.dimension;
            components = Arrays.copyOf(addendum.components, addendum.dimension);
            return;
        }

        if (addendum.dimension != dimension)
            throw new IllegalArgumentException("Addenda's dimensionalities must be the same!");

        for(int i = 0; i < dimension; i++)
            components[i] -= addendum.components[i];
    }

    public void scalarFactor(double factor) {
        for(int i = 0; i < dimension; i++)
            components[i] *= factor;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(dimension);

        for(double val : components)
            dataOutput.writeDouble(val);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dimension = dataInput.readInt();

        components = new double[dimension];
        for(int i = 0; i < dimension; i++)
            components[i] = dataInput.readDouble();
    }

    public String toString() {
        StringBuilder out = new StringBuilder();

        for(int i = 0; i < components.length; i++) {
            if(i != 0)
                out.append(",");

            out.append(components[i]);
        }

        return out.toString();
    }

    /**
     * Compute the value of || a - b || using norm h
     * @param a A point
     * @param b Another point
     * @param h type of norm, only h = 1, 2, ... are supported
     * @return the result of the computation
     */
    public static double distance(final Point a, final Point b, int h) {
        if (h <= 0)
            throw new IllegalArgumentException("I'm sorry Dave, I'm afraid I cannot do that");

        if(a.dimension != b.dimension || a.dimension == 0)
            throw new IllegalArgumentException("Incompatible dimensionality!!!");

        double m = 0;

        for(int i = 0; i < a.dimension; i++)
            m += Math.pow(Math.abs(a.components[i] - b.components[i]), h);

        // root h-th
        m = Math.pow(m, 1d/h);
        return m;
    }

    /**
     * Creates a new point from strings
     * @param centroid_comps_str An array in which each component is a string
     *                           representing a double, the i-th string will be
     *                           parsed and become the vector's i-th component
     * @return A point
     */
    public static Point fromString(String[] centroid_comps_str) {
        double[] centroid_comps = new double[centroid_comps_str.length];

        for(int j = 0; j < centroid_comps.length; j++)
            centroid_comps[j] = Double.parseDouble(centroid_comps_str[j]);

        return new Point(centroid_comps);
    }

    public static Point fromString(String str) {
        return fromString(str.split(","));
    }

    public static void main(String[] argv) {
        Point a = new Point(new double[]{1, 1});
        Point b = new Point(new double[]{2, 2});

        System.out.println(a);
        System.out.println(b);

        System.out.println(Point.distance(a,b,2) + "\t" + Math.sqrt(2.0));

        a.add(b);
        System.out.println(a);

        Point c = new Point();
        c.add(a);
        System.out.println(c);
    }

    // @TODO Average method that takes a stream of points
}
