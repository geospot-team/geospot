import Data.Query;
import Utils.FastScanner;
import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.impl.mx.VecBasedMx;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * User: Vasily
 * Date: 02.07.14
 * Time: 1:11
 */
public class Main {
    public static void main(String[] args) {
        if (args[0].equals("-help")) {
            System.out.println("Parameters: learnFilename iterations step testFilename resultsFilename");
        }
        Query[] learn = readQueries(args[0]);
        Query[] validation = readQueries(args[3]);

        int iterations = Integer.valueOf(args[1]);
        int step = Integer.valueOf(args[2]);
        YetiRank model = new YetiRank(iterations, step);
        model.fit(learn);
        List<Mx> results = model.predict(validation);
        try {
            save(results, iterations, args[4]);
        } catch (IOException e) {
            System.err.println("IOError: can't save results");

        }

    }

    private static void save(List<Mx> queries, int iterations, String filename) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        writer.write(String.format("%d %d\n", queries.size(), iterations));
        for (Mx query : queries) {
            writer.write(String.format("%d\n", query.rows()));
            for (int doc = 0; doc < query.rows(); ++doc) {
                writer.write(mkString(query.row(doc).toArray()) + "\n");
            }
            writer.flush();
        }
        writer.flush();
        writer.close();

    }

    private static String mkString(double[] arr) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < arr.length - 1; ++i) {
            builder.append(arr[i]);
            builder.append(" ");
        }
        builder.append(arr[arr.length - 1]);
        return builder.toString();
    }

    private static Query[] readQueries(String filename) {
        FastScanner scanner = new FastScanner(new File(filename));
        int queriesCount = scanner.nextInt();
        int featuresCount = scanner.nextInt();
        Query[] queries = new Query[queriesCount];
        for (int q = 0; q < queriesCount; ++q) {
            int documentsCount = scanner.nextInt();
            Mx data = new VecBasedMx(documentsCount, featuresCount);
            for (int i = 0; i < documentsCount; ++i) {
                for (int j = 0; j < featuresCount; ++j) {
                    data.set(i, j, scanner.nextDouble());
                }
            }
            double[][] weights = new double[documentsCount][documentsCount];
            for (int i = 0; i < documentsCount; ++i)
                for (int j = 0; j < documentsCount; ++j)
                    weights[i][j] = scanner.nextDouble();
            queries[q] = new Query(data, weights);
        }
        return queries;
    }
}




