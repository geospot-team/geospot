package YetiRankQueries;

import Utils.FastScanner;
import YetiRankQueries.Optimization.Query;
import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.commons.math.vectors.impl.mx.VecBasedMx;
import com.spbsu.commons.math.vectors.impl.vectors.ArrayVec;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static Utils.Utils.mkString;

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
        double[][] results = model.predict(validation);
        try {
            save(results, iterations, args[4]);
        } catch (IOException e) {
            System.err.println("IOError: can't save results");

        }

    }

    private static void save(double[][] queries, int iterations, String filename) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        writer.write(String.format("%d %d\n", queries.length));
        for (double[] query : queries) {
            writer.write(mkString(query) + "\n");
            writer.flush();
        }
        writer.flush();
        writer.close();

    }




    private static Query[] readQueries(String filename) {
        FastScanner scanner = new FastScanner(new File(filename));
        int queriesCount = scanner.nextInt();
        int featuresCount = scanner.nextInt();
        Query[] queries = new Query[queriesCount];
        for (int q = 0; q < queriesCount; ++q) {
            int documentsCount = scanner.nextInt();
            Mx data = new VecBasedMx(documentsCount, featuresCount);
            Vec target = new ArrayVec(documentsCount);
            for (int i = 0; i < documentsCount; ++i) {
                for (int j = 0; j < featuresCount; ++j) {
                    data.set(i, j, scanner.nextDouble());
                }
                target.set(i,scanner.nextDouble());
            }
            double[][] weights = new double[documentsCount][documentsCount];
            for (int i = 0; i < documentsCount; ++i)
                for (int j = 0; j < documentsCount; ++j)
                    weights[i][j] = scanner.nextDouble();
            queries[q] = new Query(data, target,weights);
        }
        return queries;
    }
}




