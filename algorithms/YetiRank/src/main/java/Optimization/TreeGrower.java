package Optimization;

import Data.Query;
import Utils.FeatureSplitsStreamGenerator;
import Utils.Split;
import WeakLearner.ObliviousTree;
import com.spbsu.commons.math.vectors.Mx;
import com.spbsu.commons.math.vectors.MxTools;
import com.spbsu.commons.math.vectors.Vec;
import com.spbsu.commons.math.vectors.VecTools;
import com.spbsu.commons.math.vectors.impl.mx.VecBasedMx;
import com.spbsu.commons.math.vectors.impl.vectors.ArrayVec;
import com.spbsu.commons.random.RandomExt;

import java.util.Random;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 21:02
 */
public class TreeGrower {
    private final Query[] queries;
    private final FeatureSplitsStreamGenerator splitsGenerator;
    private final int depth;
    private int[][] currentLeaves;

    private int[] features;
    private double[] thresholds;
    private boolean[] used;
    private int currentLevel = 0;
    private boolean bootstrap = true;
    static RandomExt rand = new RandomExt(new Random());

    public TreeGrower(Query[] queries, FeatureSplitsStreamGenerator splitsGenerator, int depth, int featureCount) {
        this.queries = queries;
        this.splitsGenerator = splitsGenerator;
        this.depth = depth;

        currentLeaves = new int[queries.length][];
        for (int i = 0; i < currentLeaves.length; ++i)
            currentLeaves[i] = new int[queries[i].rows];

        used = new boolean[featureCount];
        features = new int[depth];
        thresholds = new double[depth];
    }


    private Split calcScore(Split candidate) {
        Mx M = new VecBasedMx(1 << currentLevel, 1 << currentLevel);
        Vec v = new ArrayVec(1 << currentLevel);

        for (int q = 0; q < queries.length; ++q) {

            int bootstrapWeight = bootstrap ? rand.nextPoisson(1) : 1;
            if (bootstrapWeight == 0) continue;

            for (int i = 0; i < queries[q].rows; ++i) {
                int newLeaf_i = queries[q].data.row(i).get(candidate.feature) > candidate.value ? currentLeaves[q][i] | 1 << currentLevel : currentLeaves[q][i];
                v.set(newLeaf_i, v.get(newLeaf_i) + bootstrapWeight * queries[q].v[i]);
                for (int j = i+1; j < queries[q].rows; ++j) {
                    int newLeaf_j = queries[q].data.row(j).get(candidate.feature) > candidate.value ? currentLeaves[q][j] | 1 << currentLevel : currentLeaves[q][j];
                    M.set(newLeaf_i, newLeaf_j, M.get(newLeaf_i, newLeaf_j) + bootstrapWeight * queries[q].M[i][j]);
                }
            }
        }
        Vec c = MxTools.multiply(MxTools.inverseLTriangle(M), v);
        candidate.score = VecTools.multiply(c, MxTools.multiply(M, c)) - 2 * VecTools.multiply(v, c);
        return candidate;
    }

    private double[] calcLeaves() {
        Mx M = new VecBasedMx(1 << currentLevel, 1 << currentLevel);
        Vec v = new ArrayVec(1 << currentLevel);

        for (int q = 0; q < queries.length; ++q) {
            for (int i = 0; i < queries[q].rows; ++i) {
                int newLeaf_i = currentLeaves[q][i];
                v.set(newLeaf_i, v.get(newLeaf_i) + queries[q].v[i]);
                for (int j = i+1; j < queries[q].rows; ++j) {
                    int newLeaf_j = currentLeaves[q][j];
                    M.set(newLeaf_i, newLeaf_j, M.get(newLeaf_i, newLeaf_j) + queries[q].M[i][j]);
                }
            }
        }
        Vec c = MxTools.multiply(MxTools.inverseLTriangle(M), v);
        return c.toArray();
    }


    private void addSplit(Split split) {
        thresholds[currentLevel] = split.value;
        features[currentLevel] = split.feature;
        for (int i = 0; i < queries.length; ++i)
            for (int j = 0; j < queries[i].data.rows(); ++j) {
                currentLeaves[i][j] = queries[i].data.row(j).get(split.feature) > split.value ?
                        currentLeaves[i][j] | 1 << currentLevel :
                        currentLeaves[i][j];
            }
    }

    public ObliviousTree fit() {
        for (int i = 0; i < depth; ++i) {
            currentLevel = i + 1;
            //TODO: check what we want with tree score — minimize or maximize  ( we minimize MSE, formula looks like non-constant part of MSE)
            Split bestSplit = splitsGenerator.generateSplits()
                    .filter(splits -> !used[splits.feature])
                    .flatMap(splits -> splits.splits.parallelStream().map(this::calcScore))
                    .min((first, second) -> first.score > second.score ? 1 : -1).get();

            used[bestSplit.feature] = true;
            addSplit(bestSplit);
        }
        return new ObliviousTree(features, thresholds, calcLeaves());
    }
}
