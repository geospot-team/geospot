package Models;

import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.ml.Func;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 0:34
 */
public class ObliviousTree extends Func.Stub {
    private final int[] features;
    private final double[] thresholds;
    private final double[] leafValues;
    private final int dim;

    public ObliviousTree(int[] features, double[] thresholds, double[] leafValues) {
        this.features = features;
        this.thresholds = thresholds;
        this.leafValues = leafValues;
        this.dim = features.length;
    }



    @Override
    public double value(Vec featuresVector) {
        int leaf = 0;
        for (int i = 0; i < features.length; ++i) {
            if (featuresVector.get(features[i]) > thresholds[i]) {
                leaf = leaf | (1 << i);
            }
        }
        return leafValues[leaf];
    }

    @Override
    public int dim() {
        return dim;
    }
}
