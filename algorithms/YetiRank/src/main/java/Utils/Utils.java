package Utils;

import java.util.Random;

/**
 * User: Vasily
 * Date: 25.04.14
 * Time: 23:03
 */
public class Utils {
    static Random random = new Random();


    static public double[] rank(double[] values) {
        int[] order = argsort(values);
        double rk = 1.0;
        double[] result = new double[values.length];
        double prev = values[order[0]];
        for (int ind : order) {
            if (values[ind] != prev) {
                ++rk;
                prev = values[ind];
            }
            result[ind] = rk;
        }
        return result;
    }

    public static String mkString(double[] arr) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < arr.length - 1; ++i) {
            builder.append(arr[i]);
            builder.append(" ");
        }
        builder.append(arr[arr.length - 1]);
        return builder.toString();
    }


    //return argsort of i'th column
    public static int[] argsort(double[] values) {
        int[] order = new int[values.length];
        for (int i = 0; i < values.length; ++i)
            order[i] = i;
        qsort(values, order, 0, order.length - 1);
        return order;
    }


    private static void qsort(double[] values, int[] order, int left, int right) {
        int l = left;
        int r = right;
        if (r - l <= 0) {
            return;
        }
        if (r - l < 8) {
            for (int j = l; j <= r; ++j) {
                for (int k = j + 1; k <= r; ++k) {
                    if (values[order[j]] > values[order[k]])
                        swap(j, k, order);
                }
            }
            return;
        }

        int mid = l + random.nextInt(r - l);
        double pivot = values[order[mid]];

        while (l < r) {
            while (values[order[l]] < pivot) {
                l++;
            }
            while (values[order[r]] > pivot) {
                r--;
            }
            if (l <= r) {
                swap(l, r, order);
                l++;
                r--;
            }
        }
        if (r > left) {
            qsort(values, order, left, r);
        }
        if (l < right) {
            qsort(values, order, l, right);
        }
    }

    private static void swap(int l, int r, int[] order) {
        int tmp = order[l];
        order[l] = order[r];
        order[r] = tmp;
    }


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

    public static int[] sample(int n) {
        int[] result = new int[n];
        for (int i=0;i<result.length;++i) {
            result[i] = i;
        }
        shuffle(result);
        return result;
    }

    public static void shuffle(int[] index) {
        for (int i=index.length-1;i>0;--i) {
            int j = random.nextInt(i+1);
            swap(i,j,index);
        }
    }

}
