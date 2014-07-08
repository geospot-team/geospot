package Utils;

/**
 * User: Vasily
 * Date: 08.07.14
 * Time: 15:33
 */
public class MatrixUtils {
    public static double[] rowMean(double[][] matrix) {
        double[] result = new double[matrix[0].length];
        for (int i = 0; i < matrix[0].length; ++i) {
            for (int j = 0; j < matrix.length; ++j)
                result[i] += matrix[j][i];
            result[i] /= matrix.length;
        }
        return result;

    }

    public static double[] colMean(double[][] matrix) {
        double[] result = new double[matrix.length];
        for (int row = 0; row < matrix.length; ++row) {
            for (int col = 0; col < matrix[row].length; ++col) {
                result[row] += matrix[row][col];
            }
            result[row] /= matrix[row].length;
        }
        return result;

    }
}
