import Data.Query;
import Optimization.TreeGrower;
import Utils.FeatureSplitsStreamGenerator;
import Utils.MedianGridSplits;
import Utils.ParallelPrecompute;
import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.ml.Trans;
import com.spbsu.ml.func.Ensemble;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 0:55
 */
public class YetiRank {
    final int iterations;
    final double step;
    final int maxThreads = 4;
    final int maxLevels = 32;
    final int maxDepth = 6;
    final ForkJoinPool pool = new ForkJoinPool(maxThreads);
    private Ensemble ensemble;


    public YetiRank(int iterations, double step) {
        this.iterations = iterations;
        this.step = step;
    }

    public void fit(Query[] queries) {
        List<Trans> weakModels = new ArrayList<Trans>(iterations);
        int features = queries[0].data.columns();
        FeatureSplitsStreamGenerator splitsGenerator = new MedianGridSplits(queries, maxLevels);
        for (int t = 0; t < iterations; t++) {
            final Trans weakModel = (new TreeGrower(queries, splitsGenerator, maxDepth, features)).fit();
            weakModels.add(weakModel);
            pool.invoke(new ParallelPrecompute(queries, 0, queries.length, weakModel, step));
        }
        ensemble = new Ensemble(weakModels, -step);
    }

    public List<Mx> predict(Query[] queries) {
        List<Mx> results = new ArrayList<>(queries.length);
        for (Query query : queries) {
            results.add(ensemble.transAll(query.data));
        }
        return results;
    }
}
