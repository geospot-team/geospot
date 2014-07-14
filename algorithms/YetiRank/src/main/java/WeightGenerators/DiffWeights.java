package WeightGenerators;

import com.spbsu.ml.data.DataSet;

import java.util.function.Function;

/**
 * User: Vasily
 * Date: 08.07.14
 * Time: 15:18
 */
public class DiffWeights implements Function<DataSet, double[][]> {
    @Override
    public double[][] apply(DataSet ds) {
        double totalWeight = 0;
        double[][] weights = new double[ds.data().rows()][ds.data().rows()];
        for (int i = 0; i < weights.length; ++i) {
            for (int j = 0; j < weights.length; ++j) {
                double first = ds.target().get(i);
                double second = ds.target().get(j);
                if (first < second) {
                    weights[i][j] = Math.log(1 + first - second);
                    totalWeight += weights[i][j];//target[j] - target[i];

                } else {
                    weights[i][j] = 0;
                }
            }
        }
        for (int i = 0; i < weights.length; ++i)
            for (int j = i + 1; j < weights.length; ++j) {
                weights[i][j] /= totalWeight;
                weights[j][i] /= totalWeight;
            }
        return weights;
    }


}
