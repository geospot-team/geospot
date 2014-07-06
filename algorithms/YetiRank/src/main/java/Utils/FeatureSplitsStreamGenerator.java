package Utils;

import Data.Splits;

import java.util.stream.Stream;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 21:30
 */
public interface FeatureSplitsStreamGenerator {
    public Stream<Splits> generateSplits();

}
