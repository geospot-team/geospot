package YetiRank;

import Utils.FastScanner;
import WeightGenerators.DiffWeights;
import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.commons.math.vectors.impl.mx.VecArrayMx;
import com.spbsu.commons.math.vectors.impl.vectors.ArrayVec;
import com.spbsu.ml.data.DataSet;
import com.spbsu.ml.data.impl.DataSetImpl;
import com.spbsu.ml.data.tools.DataTools;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import static Utils.MatrixUtils.rowMean;
import static Utils.NDCG.dcg;
import static Utils.NDCG.ndcgRanks;
import static Utils.Utils.*;

/**
 * User: Vasily
 * Date: 02.07.14
 * Time: 1:11
 */
public class AreasRanking {
    static int cvCount = 20;
    static int testCount = 40;
    static int testQueries = 50;
    static int querySize = 15;

    static int k = 10;

    static String filename = "learn";
    static Random rand = new Random(10);
    static int iterations = 1500;
    static double step = 1e-2;
    static Function<DataSet, double[][]> weightFunction = new DiffWeights();

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

        Vec[] dataVec = new Vec[data.length];
        for (int i = 0; i < data.length; ++i)
            dataVec[i] = new ArrayVec(data[i]);
        Mx dataMx = new VecArrayMx(dataVec);
        Vec target = new ArrayVec(ratings);
        double[][] results = crossValidation(new DataSetImpl(dataMx, target));
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


    private static double[][] crossValidation(DataSetImpl ds) {
        double[][] scores = new double[cvCount][];
        for (int cv = 0; cv < cvCount; ++cv) {
            long startTime = System.currentTimeMillis();
            System.out.println("Starting cv iteration");

            int[] index = sample(ds.data().rows());
            int[] learnIndex = new int[index.length - testCount];
            int[] testIndex = new int[testCount];
            DataSet learn = DataTools.getSubset(ds, learnIndex);
            YetiRank model = new YetiRank(iterations, step);
            model.fit(learn, weightFunction);
            scores[cv] = calcScore(ds, testIndex, model);
            System.out.println(String.format("CV iteration working time: %d\nFor cv iteration %d scores are %s\n", (System.currentTimeMillis() - startTime) / 1000, cv, mkString(scores[cv])));
        }
        return scores;
    }

    private static double[] calcScore(DataSetImpl ds, int[] testIndex, YetiRank model) {
        DataSet[] queries = generateTest(ds, testIndex);
        double[][] scores = new double[queries.length][];
        for (int i=0;i<queries.length;++i) {
            scores[i] = ndcg(queries[i],model);
        }
        return rowMean(scores);
    }

    private static double[] ndcg(DataSet query, YetiRank model) {
        //first index — iteration, second — query
        double[] groundTruth = query.target().toArray();
        double[] ranks = rank(groundTruth);
        double best = dcg(groundTruth,groundTruth,k);

        double[][] predictions = model.predict(query.data());
        double[] result = new double[iterations];
        for (int i=0;i<iterations;++i)   {
            result[i] = ndcgRanks(ranks,predictions[i],best,k);
        }
        return result;
    }

    private static DataSet[] generateTest(DataSetImpl ds, int[] testIndex) {
        DataSet[] test = new DataSet[testQueries];
        for (int i = 0; i < testCount; ++i) {
            shuffle(testIndex);
            int[] index = new int[querySize];
            System.arraycopy(testIndex, 0, index, 0, querySize);
            test[i] = DataTools.getSubset(ds, index);
        }
        return test;

    }
}




