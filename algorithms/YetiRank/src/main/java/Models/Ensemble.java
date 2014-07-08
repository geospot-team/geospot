package Models;

import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.Vec;

/**
 * User: Vasily
 * Date: 06.07.14
 * Time: 20:12
 */
public class Ensemble {
    public final ObliviousTree[] models;
    public double weight;

    public Ensemble(ObliviousTree[] models, double weight) {
        this.models = models;
        this.weight = weight;
    }


    public double[][] predict(Mx data) {
        double result[][] = new double[data.rows()][];
        for (int i = 0; i < result.length; ++i)
            result[i] = predict(data.row(i));
        return transpose(result);
    }

    private double[][] transpose(double[][] data) {
        double[][] result = new double[data[0].length][data.length];
        for (int i = 0; i < result.length; ++i)
            for (int j = 0; j < result[0].length; ++j)
                result[i][j] = data[j][i];
        return result;
    }


    public double[] predict(Vec features) {
        double[] result = new double[models.length];
        result[0] = models[0].value(features);
        for (int i = 1; i < models.length; ++i) {
            result[i] = result[i - 1] + weight * models[i].value(features);
        }
        return result;
    }
}
