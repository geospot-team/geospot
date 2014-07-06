package Data;

import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.commons.math.vectors.VecTools;
import com.spbsu.commons.math.vectors.impl.vectors.ArrayVec;
import com.spbsu.ml.Trans;

import java.util.Arrays;

import static Utils.Utils.argsort;
import static Utils.Utils.rank;
import static java.lang.Math.exp;
import static java.lang.Math.pow;


/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 0:09
 */

//TODO: Mx implementation on double arrays (to save memory on queries)
public class Query {
    public final Mx data;
    public final Vec target;
    public final double[][] weights;
    public final int rows;
    public final int k = 10;

    //precomputed values
    private Vec currentPredictions;
    private final double bestDCG;
    private final double[] ranks;

    public double[][] M;
    public double[] v;

    public Query(Mx data, Vec target, double[][] weights) {
        this.data = data;
        this.weights = weights;
        this.target = target;
        M = new double[data.rows()][data.rows()];
        rows = data.rows();
        v = new double[data.rows()];
        currentPredictions = new ArrayVec(data.rows());

        double[] targetArr = target.toArray();
        bestDCG = dcg(targetArr, targetArr, k);
        ranks = rank(targetArr);
        precompute();
    }


    public Query(Mx data, Vec target) {
        this(data, target, ndcgWeights(target.toArray()));
    }


    public void precompute(Trans weak, double step) {
        VecTools.append(currentPredictions, VecTools.scale(weak.transAll(data), -step));
        //clear
        precompute();
    }

    private void precompute() {
        for (double[] row : M) Arrays.fill(row, 0);
        Arrays.fill(v, 0);
        //precompute
        for (int i = 0; i < M.length; ++i)
            for (int j = 0; j < M.length; ++j) {
                if (weights[i][j] == 0)
                    continue;
                v[i] -= weights[i][j] / (exp(currentPredictions.get(i) - currentPredictions.get(j)) + 1);
                v[j] += weights[i][j] / (exp(currentPredictions.get(i) - currentPredictions.get(j)) + 1);
                M[i][i] += weights[i][j];
                M[i][j] -= weights[i][j];
                M[j][i] -= weights[i][j];
                M[j][j] += weights[i][j];
            }
    }


    public double ndcg(double[] predictions) {
        return dcg(predictions) / bestDCG;

    }

    public double dcg(double[] predictions) {
        int[] predictionsOrder = argsort(predictions);
        double[] relevance = new double[k];
        for (int i = 0; i < relevance.length; ++i) {
            relevance[i] = (ranks.length - ranks[predictionsOrder[i]] + 1) / ranks.length;
        }
        double result = 0;

        for (int i = 0; i < relevance.length; ++i) {
            result += (pow(2, relevance[i]) - 1) / Math.log(i + 2);
        }
        result *= Math.log(2);
        return result;

    }


    public static double ndcg(double[] real, double[] predictions, int k) {
        double best = dcg(real, real, k);
        double pred = dcg(real, predictions, k);
        return pred / best;
    }


    public static double dcg(double[] real, double[] predictions, int k) {
        double[] ranks = rank(real);
        return dcgRanks(ranks, predictions, k);
    }

    private static double dcgRanks(double[] ranks, double[] predictions, int k) {
        int[] predictionsOrder = argsort(predictions);
        double[] relevance = new double[k];
        for (int i = 0; i < relevance.length; ++i) {
            relevance[i] = (ranks.length - ranks[predictionsOrder[i]] + 1) / ranks.length;
        }
        double result = 0;

        for (int i = 0; i < relevance.length; ++i) {
            result += (pow(2, relevance[i]) - 1) / Math.log(i + 2);
        }
        result *= Math.log(2);
        return result;
    }

    private static double ndcgRanks(double[] ranks, double[] predictions, double best, int k) {
        double pred = dcgRanks(ranks, predictions, k);
        return pred / best;
    }


    public static double[][] ndcgWeights(double[] target) {
        return ndcgWeights(target, target.length);
    }

    public static double[][] ndcgWeights(double[] target, int k) {
        double[] ranks = rank(target);
        double bestDCG = dcgRanks(ranks, target, k);
        double totalWeight = 0;
        double[][] weights = new double[target.length][target.length];
        for (int i = 0; i < target.length; ++i) {
            for (int j = 0; j < target.length; ++j) {
                if (target[i] < target[j]) {
                    swap(i, j, target);
                    double diff = 1.0 - ndcgRanks(ranks, target, bestDCG, k);
                    swap(i, j, target);
                    weights[i][j] = exp(-2 * diff);
                    totalWeight += weights[i][j];
                } else {
                    weights[i][j] = 0;
                }
            }
        }
        for (int i = 0; i < target.length; ++i)
            for (int j = i + 1; j < target.length; ++j) {
                weights[i][j] /= totalWeight;
                weights[j][i] /= totalWeight;
            }
        return weights;
    }

    private static void swap(int i, int j, double[] target) {
        double tmp = target[i];
        target[i] = target[j];
        target[j] = tmp;
    }

}
