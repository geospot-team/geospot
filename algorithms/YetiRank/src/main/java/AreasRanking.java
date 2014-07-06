import Data.Query;
import Utils.FastScanner;
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
    static int queriesCount = 300;
    static int cvCount = 100;
    static int testCount = 20;
    static int areasPerQuery = 10;
    static String filename = "learn";
    static Random rand = new Random(10);
    static int iterations = 500;
    static double step = 1e-1;

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
            for (int cv = 0; cv < cvCount; ++cv) {
                mean[i] += results[cv][i];
            }
            mean[i] /= cvCount;
        }

        System.out.println("Mean cv scores on iterations: " + mkString(mean) + "\n");
        double max = 0;
        double maxInd = 0;
        for (int i=0;i<mean.length;++i)
            if (mean[i] > max) {
                max = mean[i];
                maxInd = i;
            }
        System.out.println(String.format("Max mean value is %f on iteration %f \n",max,maxInd));

    }


    private static double[][] crossValidation(double[][] data, double[] ratings) {
        double[][] scores = new double[cvCount][];
        for (int cv = 0; cv < cvCount; ++cv) {
            long startTime = System.currentTimeMillis();
            System.out.println("Starting cv iteration");
            int[] index = sample(data.length);
            Query[] learn = makeLearn(data, ratings, index);
            Query test = makeTest(data, ratings, index);
            YetiRank model = new YetiRank(iterations, step);
            model.fit(learn);
            scores[cv] = model.ndcg(test);
            System.out.println(String.format("CV iteration working time: %d\nFor cv iteration %d scores are %s\n",(System.currentTimeMillis() - startTime) / 1000,cv,mkString(scores[cv])));
        }
        return scores;
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




