package YetiRank.Optimization;

import Models.ObliviousTree;
import Utils.FeatureSplitsStreamGenerator;
import Utils.Split;
import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.MxTools;
import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.commons.math.vectors.VecTools;
import com.spbsu.commons.math.vectors.impl.mx.VecBasedMx;
import com.spbsu.commons.math.vectors.impl.vectors.ArrayVec;
import com.spbsu.commons.random.RandomExt;
import com.spbsu.ml.data.DataSet;

import java.util.Arrays;
import java.util.Random;

import static java.lang.Math.exp;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 21:02
 */
public class TreeGrower {
    private final DataSet ds;
    private final Vec target;
    private final double[][] weights;
    private final FeatureSplitsStreamGenerator splitsGenerator;
    private final int depth;
    private int[] currentLeaves;

    private int[] features;
    private double[] thresholds;
    private boolean[] used;
    private int currentLevel = 0;
    private final int observationsCount;
    private final int featuresCount;
    static RandomExt rand = new RandomExt(new Random());

    //precomputed
    private double[][] precomputedM;
    private double[] precomputedV;
    private boolean bootstrap = true;

    public TreeGrower(DataSet ds, Vec currentTarget, FeatureSplitsStreamGenerator splitsGenerator, int depth, double[][] weights) {
        this.weights = weights;
        this.target = currentTarget;
        this.ds = ds;
        this.splitsGenerator = splitsGenerator;
        this.depth = depth;
        this.featuresCount = ds.data().columns();
        this.observationsCount = ds.data().rows();

        currentLeaves = new int[ds.data().rows()];
        used = new boolean[featuresCount];
        features = new int[depth];
        thresholds = new double[depth];
        precomputedM = new double[observationsCount][observationsCount];
        precomputedV = new double[observationsCount];
    }


    private Split calcScore(Split candidate) {
        Mx M = new VecBasedMx(1 << (currentLevel + 1), 1 << (currentLevel + 1));
        Vec v = new ArrayVec(1 << (currentLevel + 1));
        for (int i = 0; i < observationsCount; ++i) {
            int newLeaf_i = ds.data().row(i).get(candidate.feature) > candidate.value ? currentLeaves[i] | 1 << currentLevel : currentLeaves[i];
            v.set(newLeaf_i, v.get(newLeaf_i) + precomputedV[i]);
            for (int j = 0; j < observationsCount; ++j) {
                int newLeaf_j = ds.data().row(j).get(candidate.feature) > candidate.value ? currentLeaves[j] | 1 << currentLevel : currentLeaves[j];
                M.set(newLeaf_i, newLeaf_j, M.get(newLeaf_i, newLeaf_j) + precomputedM[i][j]);
            }
        }
        Vec c = MxTools.multiply(MxTools.inverseLTriangle(M), v);
        candidate.score = VecTools.multiply(c, MxTools.multiply(M, c)) - 2 * VecTools.multiply(v, c);
        return candidate;
    }

    private double[] calcLeaves() {
        Mx M = new VecBasedMx(1 << (currentLevel + 1), 1 << (currentLevel + 1));
        Vec v = new ArrayVec(1 << (currentLevel + 1));
        for (int i = 0; i < observationsCount; ++i) {
            int newLeaf_i = currentLeaves[i];
            v.set(newLeaf_i, v.get(newLeaf_i) + precomputedV[i]);
            for (int j = 0; j < observationsCount; ++j) {
                int newLeaf_j = currentLeaves[j];
                M.set(newLeaf_i, newLeaf_j, M.get(newLeaf_i, newLeaf_j) + precomputedM[i][j]);
            }
        }
        Vec c = MxTools.multiply(MxTools.inverseLTriangle(M), v);
        return c.toArray();
    }


    private void addSplit(Split split) {
        thresholds[currentLevel] = split.value;
        features[currentLevel] = split.feature;
        for (int i = 0; i < observationsCount; ++i) {
            currentLeaves[i] = ds.data().row(i).get(split.feature) > split.value ?
                    currentLeaves[i] | 1 << currentLevel :
                    currentLeaves[i];
        }
    }

    public ObliviousTree fit() {
        precompute();
        for (int i = 0; i < depth; ++i) {
            currentLevel = i;
            Split bestSplit = splitsGenerator.generateSplits()
                    .filter(splits -> !used[splits.feature])
                    .flatMap(splits -> splits.splits.parallelStream().map(this::calcScore))
                    .min((first, second) -> first.score < second.score ? -1 : 1).get();

            used[bestSplit.feature] = true;
            addSplit(bestSplit);
        }
        return new ObliviousTree(features, thresholds, calcLeaves());
    }

    private void precompute() {
        //clear
        for (double[] row : precomputedM) Arrays.fill(row, 0);
        Arrays.fill(precomputedV, 0);
        //precompute
        for (int i = 0; i < precomputedM.length; ++i) {
            int bootstrapWeight = bootstrap ? rand.nextPoisson(1) : 1;
            if (bootstrapWeight == 0)
                continue;
            for (int j = 0; j < precomputedM.length; ++j) {
                if (weights[i][j] == 0)
                    continue;
                double tmp = bootstrapWeight * weights[i][j] / (exp(target.get(i) - target.get(j)) + 1);
                precomputedV[i] -= tmp;
                precomputedV[j] += tmp;
                precomputedM[i][i] += bootstrapWeight * weights[i][j];
                precomputedM[i][j] -= bootstrapWeight * weights[i][j];
                precomputedM[j][i] -= bootstrapWeight * weights[i][j];
                precomputedM[j][j] += bootstrapWeight * weights[i][j];
            }
        }
    }
}
