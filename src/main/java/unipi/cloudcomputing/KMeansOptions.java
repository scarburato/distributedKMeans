package unipi.cloudcomputing;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Just command line options manager from Apache
 */
public class KMeansOptions extends Options {
    public static final String cmdstring = "-i INPUT -o OUTPUT -d DIMENSIONALITY" +
                                           "[-t THRESHOLD] [-k CLUSTERS] [-e THRESHOLD]" +
                                           "[-I MAX_ITERATIONS]" +
                                           "[-c centroid1;[centroid2...]]" ;

    public KMeansOptions() {
        Option input = new Option("i", "input", true, "Input file from HadoopFS");
        input.setRequired(true);
        addOption(input);

        Option output = new Option("o", "output", true, "Output file from HadoopFS");
        output.setRequired(true);
        addOption(output);

        Option dim = new Option("d", "dimensionality", true, "Data dimensionality");
        dim.setRequired(true);
        addOption(dim);

        Option norm = new Option("n", "norm", true, "Norm type n = 1, 2, ...");
        norm.setRequired(false);
        addOption(norm);

        Option clusters = new Option("k", "clusters", true, "Number of clusters to generate");
        clusters.setRequired(false);
        addOption(clusters);

        Option threshold = new Option("e", "threshold", true, "Epsilon for stop condition");
        threshold.setRequired(false);
        addOption(threshold);

        Option iterations = new Option("I", "maxiterations", true, "Maximum iterations");
        iterations.setRequired(false);
        addOption(iterations);

        Option centroids = new Option("c", "centroids", true, "Provide k starting centroids separated by `;`");
        centroids.setRequired(false);
        addOption(centroids);
    }
}
