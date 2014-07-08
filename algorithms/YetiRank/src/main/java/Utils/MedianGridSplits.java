package Utils;

import YetiRankQueries.Optimization.Query;
import com.spbsu.ml.data.DataSet;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 20:54
 */


public class MedianGridSplits implements FeatureSplitsStreamGenerator {

    private final double[][] data;
    private List<Splits> splits;
    private final int[] levels;

    private final int featuresCount;
    private final int binFactor;
    private final Random rand = new Random();

    private int[] index;
    private int[][] sortedIndex;


    public MedianGridSplits(DataSet ds, int binFactor) {
        this.binFactor = binFactor;
        featuresCount = ds.data().columns();
        data = new double[featuresCount][];
        for (int feature = 0; feature < featuresCount; ++feature) {
            data[feature] = ds.data().col(feature).toArray();
        }

        splits = new ArrayList<>(featuresCount); // splits — array for candidate splits for every feature
        levels = new int[featuresCount];

        init();


    }

    private void init() {
        this.index = new int[data[0].length];
        for (int i = 0; i < this.index.length; ++i) {
            this.index[i] = i;
        }

        this.sortedIndex = new int[data.length][];
        for (int i = 0; i < data.length; ++i) {
            this.sortedIndex[i] = argsort(i);
        }

        calcLevels();

        for (int feature = 0; feature < featuresCount; ++feature) {
            List<Split> featureSplits = new ArrayList<>(min(binFactor, levels[feature]));
            splits.add(feature, new Splits(featureSplits, feature));
        }

        calcSplits();
    }

    public MedianGridSplits(Query[] queries, int binFactor) {

        this.binFactor = binFactor;
        featuresCount = queries[0].data.columns();
        splits = new ArrayList<>(featuresCount); // splits — array for candidate splits for every feature
        levels = new int[featuresCount];

        int documentsCount = 0;
        for (Query query : queries) documentsCount += query.rows;

        data = new double[featuresCount][documentsCount];


        int currentCol = 0;

        for (Query query : queries) {
            for (int j = 0; j < query.rows; ++j, ++currentCol) {
                for (int featureInd = 0; featureInd < featuresCount; ++featureInd) {
                    data[featureInd][currentCol] = query.data.row(j).get(featureInd);
                }
            }
        }

        init();

    }

    private void calcSplits() {
        for (int feature = 0; feature < featuresCount; ++feature) {
            quantilize(feature);
        }
    }

    private void quantilize(int feature) {
        int[] order = sortedIndex[feature];
        Splits featureSplits = splits.get(feature);

        if (levels[feature] < binFactor) {
            Split current = new Split(feature, data[feature][order[0]], Integer.MAX_VALUE);
            for (int i = 1; i < data[feature].length; ++i) {
                if (data[feature][order[i]] != data[feature][order[i - 1]]) {
                    featureSplits.splits.add(current);
                    current = new Split(feature, data[feature][order[i]], Integer.MAX_VALUE);
                }
            }
        } else {
            //binarize by median
            double[] observations = new double[data[feature].length];
            for (int i = 0; i < observations.length; ++i) {
                observations[i] = data[feature][order[i]];
            }
            double[] borders = calcBorders(observations);
            for (double border : borders) {
                featureSplits.splits.add(new Split(feature, border, Integer.MAX_VALUE));
            }
        }
    }


    //code from jmll grid tools
    private double[] calcBorders(double[] feature) {

        TIntArrayList borders = new TIntArrayList();
        borders.add(feature.length);
        while (borders.size() < binFactor + 1) {
            double bestScore = 0;
            int bestSplit = -1;
            for (int i = 0; i < borders.size(); i++) {
                int start = i > 0 ? borders.get(i - 1) : 0;
                int end = borders.get(i);
                double median = feature[start + (end - start) / 2];
                int split = Math.abs(Arrays.binarySearch(feature, start, end, median));

                while (split > 0 && Math.abs(feature[split] - median) < 1e-9) // look for first less then median value
                    split--;
                if (Math.abs(feature[split] - median) > 1e-9) split++;
                final double scoreLeft = Math.log(end - split) + Math.log(split - start);
                if (split > 0 && scoreLeft > bestScore) {
                    bestScore = scoreLeft;
                    bestSplit = split;
                }
                while (++split < end && Math.abs(feature[split] - median) < 1e-9)
                    ; // first after elements with such value
                final double scoreRight = Math.log(end - split) + Math.log(split - start);
                if (split < end && scoreRight > bestScore) {
                    bestScore = scoreRight;
                    bestSplit = split;
                }
            }
            if (bestSplit < 0)
                break;
            borders.add(bestSplit);
            borders.sort();
        }

        double[] bordersValues = new double[borders.size() - 1];
        for (int i = 0; i < borders.size() - 1; ++i) {
            bordersValues[i] = borders.get(i);

        }

        return bordersValues;
    }


    private void calcLevels() {
        for (int feature = 0; feature < featuresCount; ++feature) {
            int differentValues = 1;
            for (int i = 1; i < data[feature].length; ++i) {
                if (data[feature][sortedIndex[feature][i]] != data[feature][sortedIndex[feature][i - 1]])
                    differentValues++;
            }
            levels[feature] = differentValues;
        }
    }

    private int max(int a, int b) {
        return a > b ? a : b;
    }

    private int min(int a, int b) {
        return a < b ? a : b;
    }


    //return argsort of i'th column
    public int[] argsort(int i) {
        int[] order = new int[index.length];
        System.arraycopy(index, 0, order, 0, index.length);
        qsort(i, order, 0, order.length - 1);
        return order;
    }

    public int[] argsort(int i, int[] index) {
        int[] order = new int[index.length];
        System.arraycopy(index, 0, order, 0, index.length);
        qsort(i, order, 0, order.length - 1);
        return order;
    }


    private void qsort(int i, int[] order, int left, int right) {
        int l = left;
        int r = right;
        if (r - l <= 0) {
            return;
        }
        if (r - l < 8) {
            for (int j = l; j <= r; ++j) {
                for (int k = j + 1; k <= r; ++k) {
                    if (data[i][order[j]] > data[i][order[k]])
                        swap(j, k, order);
                }
            }
            return;
        }

        int mid = l + rand.nextInt(r - l);
        double pivot = data[i][order[mid]];

        while (l < r) {
            while (data[i][order[l]] < pivot) {
                l++;
            }
            while (data[i][order[r]] > pivot) {
                r--;
            }
            if (l <= r) {
                swap(l, r, order);
                l++;
                r--;
            }
        }
        if (r > left) {
            qsort(i, order, left, r);
        }
        if (l < right) {
            qsort(i, order, l, right);
        }
    }


    private void swap(int l, int r, int[] order) {
        int tmp = order[l];
        order[l] = order[r];
        order[r] = tmp;
    }


    @Override
    public Stream<Splits> generateSplits() {
        return splits.parallelStream();
    }

}
