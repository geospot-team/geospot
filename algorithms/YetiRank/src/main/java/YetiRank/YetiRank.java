package YetiRank;

import Models.Ensemble;
import Models.ObliviousTree;
import Utils.FeatureSplitsStreamGenerator;
import Utils.MedianGridSplits;
import YetiRank.Optimization.TreeGrower;
import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.commons.math.vectors.VecTools;
import com.spbsu.commons.math.vectors.impl.vectors.ArrayVec;
import com.spbsu.ml.data.DataSet;

import java.util.function.Function;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 0:55
 */
public class YetiRank {
    final int iterations;
    final double step;
    final int maxLevels = 32;
    final int maxDepth = 6;
    private Ensemble ensemble;



    public YetiRank(int iterations, double step) {
        this.iterations = iterations;
        this.step = step;
    }

    public void fit(DataSet ds, Function<DataSet,double[][]> generator)  {
        fit(ds, generator.apply(ds));

    }
    public void fit(DataSet ds,double[][] weights) {
        ObliviousTree[] weakModels = new ObliviousTree[iterations];
        int features = ds.data().columns();
        FeatureSplitsStreamGenerator splitsGenerator = new MedianGridSplits(ds, maxLevels);
        Vec target = new ArrayVec(ds.data().rows());
        for (int t = 0; t < iterations; t++) {
            final ObliviousTree weakModel = (new TreeGrower(ds, target,splitsGenerator, maxDepth,weights)).fit();
            weakModels[t] = weakModel;
            VecTools.append(target, VecTools.scale(weakModel.transAll(ds.data()), step));
        }
        ensemble = new Ensemble(weakModels, step);
    }

    public double[] predict(Vec observation) {
        return ensemble.predict(observation);
    }

    public double[][] predict(Mx query) {
        return ensemble.predict(query);
    }


}
