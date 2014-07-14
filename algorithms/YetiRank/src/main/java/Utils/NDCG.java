package Utils;

import static Utils.Utils.argsort;
import static Utils.Utils.rank;
import static java.lang.Math.pow;

/**
 * User: Vasily
 * Date: 08.07.14
 * Time: 10:44
 */
public class NDCG  {
    public static double ndcg(double[] real, double[] predictions, int k) {
        double best = dcg(real, real, k);
        double pred = dcg(real, predictions, k);
        return pred / best;
    }


    public static double dcg(double[] real, double[] predictions, int k) {
        double[] ranks = rank(real);
        return dcgRanks(ranks, predictions, k);
    }

    public static double dcgRanks(double[] ranks, double[] predictions, int k) {
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

    public static double ndcgRanks(double[] ranks, double[] predictions, double best, int k) {
        double pred = dcgRanks(ranks, predictions, k);
        return pred / best;
    }
}
