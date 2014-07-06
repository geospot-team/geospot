package Data;

import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.commons.math.vectors.VecTools;
import com.spbsu.commons.math.vectors.impl.vectors.ArrayVec;
import com.spbsu.ml.Trans;

import java.util.Arrays;


/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 0:09
 */
public class Query {
    public final Mx data;
    public final double[][] weights;
    public final int rows;

    //precomputed values
    private Vec predictions;
    public double[][] M;
    public double[] v;

    public Query(Mx data, double[][] weights) {
        this.data = data;
        this.weights = weights;
        M = new double[data.rows()][data.rows()];
        rows = data.rows();
        v = new double[data.rows()];
        predictions = new ArrayVec(data.rows());
    }


    public void precompute(Trans weak, double step) {
        VecTools.append(predictions,VecTools.scale(weak.transAll(data),-step));

        //clear
        for (double[] row : M) Arrays.fill(row, 0);
        Arrays.fill(v,0);


        //precompute
        for (int i=0;i<M.length;++i)
            for (int j=0;j<M.length;++j) {
                if (i==j)
                    continue;
                v[i] -=  weights[i][j] / ( Math.exp(predictions.get(i) - predictions.get(j)) + 1);
                v[j] += weights[i][j] / ( Math.exp(predictions.get(i) - predictions.get(j)) + 1);
                M[i][i]  += weights[i][j];
                M[i][j] -= weights[i][j];
                M[j][i] -= weights[i][j];
                M[j][j] += weights[i][j];
            }
    }

}
