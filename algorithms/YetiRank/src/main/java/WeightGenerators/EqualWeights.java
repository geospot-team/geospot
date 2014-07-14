package WeightGenerators;

import com.spbsu.ml.data.DataSet;

import java.util.function.Function;

/**
 * User: Vasily
 * Date: 08.07.14
 * Time: 15:31
 */
public class EqualWeights implements Function<DataSet, double[][]> {
    @Override
    public double[][] apply(DataSet ds) {
        double[] target = ds.target().toArray();
        int k = target.length;
        double totalWeight = 0;
        double[][] weights = new double[target.length][target.length];
        for (int i = 0; i < target.length; ++i) {
            for (int j = 0; j < target.length; ++j) {
                if (target[i] < target[j]) {
                    weights[i][j] = 1; // pairs, which make big diff in ndcg are more important
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
