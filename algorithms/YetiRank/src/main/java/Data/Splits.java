package Data;

import Utils.Split;
import java.util.List;

/**
 * User: Vasily
 * Date: 03.07.14
 * Time: 23:48
 */
public class Splits {
    public final List<Split> splits;
    public final int feature;

    public Splits(List<Split> splits, int feature) {
        this.splits = splits;
        this.feature = feature;
    }


}
