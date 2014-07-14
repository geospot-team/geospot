package WeightGenerators;

import com.spbsu.ml.data.DataSet;

import java.util.function.Function;

import static Utils.NDCG.dcgRanks;
import static Utils.NDCG.ndcgRanks;
import static Utils.Utils.rank;
import static Utils.Utils.swap;

/**
 * User: Vasily
 * Date: 08.07.14
 * Time: 15:27
 */
public class NdcgWeights implements Function<DataSet, double[][]> {
    @Override
    public double[][] apply(DataSet ds) {
        double[] target = ds.target().toArray();
        int k = target.length;
        double[] ranks = rank(target);
        double bestDCG = dcgRanks(ranks, target, k);
        double totalWeight = 0;

        double[][] weights = new double[target.length][target.length];
        for (int i = 0; i < target.length; ++i) {
            for (int j = 0; j < target.length; ++j) {
                if (target[i] < target[j]) {
                    swap(i, j, target);
                    double diff = 1.0 - ndcgRanks(ranks, target, bestDCG, k);//+ Math.log(1 + (ranks[i] - ranks[j]) / ranks.length);
                    swap(i, j, target);
                    weights[i][j] = Math.exp(-diff); // pairs, which make big diff in ndcg are more important
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


}
