package YetiRankQueries;

import Models.Ensemble;
import Models.ObliviousTree;
import Utils.FeatureSplitsStreamGenerator;
import Utils.MedianGridSplits;
import YetiRankQueries.Optimization.ParallelPrecompute;
import YetiRankQueries.Optimization.Query;
import YetiRankQueries.Optimization.TreeGrower;

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
    final int maxDepth = 5;
    final ForkJoinPool pool = new ForkJoinPool(maxThreads);
    private Ensemble ensemble;


    public YetiRank(int iterations, double step) {
        this.iterations = iterations;
        this.step = step;
    }

    public void fit(Query[] queries) {
        ObliviousTree[] weakModels = new ObliviousTree[iterations];
        int features = queries[0].data.columns();
        FeatureSplitsStreamGenerator splitsGenerator = new MedianGridSplits(queries, maxLevels);
        for (int t = 0; t < iterations; t++) {
            final ObliviousTree weakModel = (new TreeGrower(queries, splitsGenerator, maxDepth, features)).fit();
            weakModels[t] = weakModel;
            pool.invoke(new ParallelPrecompute(queries, 0, queries.length, weakModel, step));
        }

        ensemble = new Ensemble(weakModels, step);
    }

    public double[][] predict(Query[] queries) {
        double[][] results = new double[queries.length][];
        for (int q = 0; q < queries.length; ++q) {
            results[q] = ensemble.predict(queries[q].data)[iterations - 1];
        }
        return results;
    }

    public double[][] ndcg(Query[] queries) {
        double[][] result = new double[queries.length][];
        for (int i = 0; i < queries.length; ++i) {
            result[i] = ndcg(queries[i]);
        }
        return result;
    }

    public double[] ndcg(Query query) {
        double[] result = new double[iterations];
        double[][] predictions = ensemble.predict(query.data);
        for (int i = 0; i < iterations; ++i) {
            result[i] = query.ndcg(predictions[i]);
        }
        return result;
    }
}
