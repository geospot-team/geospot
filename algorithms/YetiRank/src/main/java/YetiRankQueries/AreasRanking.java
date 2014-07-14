package YetiRankQueries;

import Utils.FastScanner;
import YetiRankQueries.Optimization.Query;
import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.commons.math.vectors.impl.mx.VecBasedMx;
import com.spbsu.commons.math.vectors.impl.vectors.ArrayVec;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;

import static Utils.Utils.mkString;
import static Utils.Utils.sample;

/**
 * User: Vasily
 * Date: 02.07.14
 * Time: 1:11
 */
public class AreasRanking {
    static int queriesCount = 250;
    static int cvCount = 20;
    static int testCount = 40;
    static int testQueriesCount = 100;
    static int areasPerQuery = 15;

    static String filename = "learn";
    static Random rand = new Random(10);
    static int iterations = 1500;
    static double step = 1e-2;

    public static void main(String[] args) {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");
        System.out.println(ForkJoinPool.commonPool().getParallelism());
        FastScanner scanner = new FastScanner(new File(filename));
        int rows = scanner.nextInt();
        int features = scanner.nextInt();

        double[][] data = new double[rows][features];
        double[] ratings = new double[rows];

        for (int i = 0; i < rows; ++i) {
            for (int j = 0; j < features; ++j)
                data[i][j] = scanner.nextDouble();
            ratings[i] = scanner.nextDouble();
        }

        double[][] results = crossValidation(data, ratings);
        printMean(results);

    }

    private static void printMean(double[][] results) {
        double[] mean = new double[results[0].length];
        for (int i = 0; i < iterations; ++i) {
            for (int cv = 0; cv < results.length; ++cv) {
                mean[i] += results[cv][i];
            }
            mean[i] /= results.length;
        }

        System.out.println("Mean scores on iterations: " + mkString(mean) + "\n");
        double max = 0;
        double maxInd = 0;
        for (int i = 0; i < mean.length; ++i)
            if (mean[i] > max) {
                max = mean[i];
                maxInd = i;
            }
        System.out.println(String.format("Max mean value is %f on iteration %f \n", max, maxInd));

    }


    private static double[][] crossValidation(double[][] data, double[] ratings) {
        double[][] scores = new double[cvCount][];
        for (int cv = 0; cv < cvCount; ++cv) {
            long startTime = System.currentTimeMillis();
            System.out.println("Starting cv iteration");

            int[] index = sample(data.length);
            int[] learnIndex = new int[index.length - testCount];
            int[] testIndex = new int[testCount];
            System.arraycopy(index, 0, learnIndex, 0, index.length - testCount);
            System.arraycopy(index, index.length - testCount, testIndex, 0, testCount);
            Query[] learn = makeQueries(data, ratings, learnIndex, queriesCount);
            Query[] test = makeQueries(data, ratings, testIndex, testQueriesCount);
            YetiRank model = new YetiRank(iterations, step);
            model.fit(learn);
            scores[cv] = rowMean(model.ndcg(test));
            System.out.println(String.format("CV iteration working time: %d\nFor cv iteration %d scores are %s\n", (System.currentTimeMillis() - startTime) / 1000, cv, mkString(scores[cv])));
            printMean(model.ndcg(learn));
        }
        return scores;
    }

    private static Query[] makeQueries(double[][] data, double[] ratings, int[] index, int queriesCount) {
        Query[] queries = new Query[queriesCount];
        //learn
        for (int i = 0; i < queriesCount; ++i) {
            Mx sample = new VecBasedMx(areasPerQuery, data[0].length);
            Vec sampleRating = new ArrayVec(areasPerQuery);
            for (int k = 0; k < areasPerQuery; ++k) {
                int ind = index[rand.nextInt(index.length)];
                for (int j = 0; j < data[ind].length; ++j) {
                    sample.set(k, j, data[ind][j]);
                }
                sampleRating.set(k, ratings[ind]);
            }
            queries[i] = new Query(sample, sampleRating);
        }
        return queries;
    }

    private static double[] rowMean(double[][] table) {
        double[] result = new double[table[0].length];
        for (int i = 0; i < table[0].length; ++i) {
            for (int j = 0; j < table.length; ++j)
                result[i] += table[j][i];
            result[i] /= table.length;
        }
        return result;
    }

    private static Query makeTest(double[][] data, double[] ratings, int[] index) {
        Mx sample = new VecBasedMx(testCount, data[0].length);
        Vec sampleRating = new ArrayVec(testCount);

        for (int k = 0; k < testCount; ++k) {
            int ind = index[index.length - testCount + k];
            for (int j = 0; j < data[ind].length; ++j) {
                sample.set(k, j, data[ind][j]);
            }
            sampleRating.set(k, ratings[ind]);
        }
        return new Query(sample, sampleRating);

    }


    private static Query[] makeLearn(double[][] data, double[] ratings, int[] index) {

        int learnLimit = data.length - testCount;
        Query[] learn = new Query[queriesCount];
        //learn
        for (int i = 0; i < queriesCount; ++i) {
            Mx sample = new VecBasedMx(areasPerQuery, data[0].length);
            Vec sampleRating = new ArrayVec(areasPerQuery);

            for (int k = 0; k < areasPerQuery; ++k) {
                int ind = index[rand.nextInt(learnLimit)];
                for (int j = 0; j < data[ind].length; ++j) {
                    sample.set(k, j, data[ind][j]);
                }
                sampleRating.set(k, ratings[ind]);
            }
            learn[i] = new Query(sample, sampleRating);
        }
        return learn;
    }


}




