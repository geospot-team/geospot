package YetiRankQueries.Optimization;

import com.spbsu.ml.Trans;

import java.util.concurrent.RecursiveAction;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 1:20
 */
public class ParallelPrecompute extends RecursiveAction {
    private final Query[] queries;
    private final int sequential = 2;
    private final int left;
    private final int right;
    private final Trans weak;
    private final double step;

    public ParallelPrecompute(Query[] queries, int left, int right, Trans weak, double step) {
        this.queries = queries;
        this.left = left;
        this.right = right;
        this.weak = weak;
        this.step = step;
    }

    @Override
    protected void compute() {
        if (right - left < sequential) {
            for (int i = left; i < right; ++i)
                queries[i].precompute(weak, step);
        } else {
            int mid = (right + left) >>> 1;
            invokeAll(new ParallelPrecompute(queries, left, mid, weak, step),
                    new ParallelPrecompute(queries, mid, right,weak,step));
        }
    }
}
