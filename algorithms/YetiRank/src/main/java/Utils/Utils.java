package Utils;
import java.util.Random;

/**
 * User: Vasily
 * Date: 25.04.14
 * Time: 23:03
 */
public class Utils {
    static Random random = new Random();



    public static double kappa(int[] real, int[] predict) {
        double[][] counts = new double[3][3];
        for (int i = 0; i < real.length; ++i) {
            counts[real[i]][predict[i]]++;
        }
        double total = 0;
        for (int i = 0; i < counts.length; ++i)
            for (int j = 0; j < counts[i].length; ++j)
                total += counts[i][j];

        double pra = counts[0][0] + counts[1][1] + counts[2][2];
        pra /= total;
        double[] rowSum = new double[3];
        double[] colSum = new double[3];
        for (int i = 0; i < counts.length; ++i) {
            for (int j = 0; j < counts[i].length; ++j) {
                colSum[i] += counts[i][j];
                rowSum[j] += counts[i][j];
            }
        }

        double pre = 0;
        for (int i = 0; i < colSum.length; ++i) {
            pre += colSum[i] * rowSum[i] / (total * total);
        }

        return (pra - pre) / (1 - pre);
    }

}
