import GeoHash.GeoHashBuilder;
import GeoHash.Impl.Grid;
import GeoHash.Impl.GridGeoHashBuilder;
import GeoHash.Impl.LinearGeoHashBuilder;
import gnu.trove.TDoubleArrayList;
import gnu.trove.TIntArrayList;
import GeoHash.GeoHash;

import java.util.Arrays;
import java.util.Random;

/**
 * Created by noxoomo on 26/10/14.
 */
public class GridTest extends GeoTest {
  final double minLat = 59.829833;
  final double maxLat = 60.029015;
  final double minLon =  30.178096;
  final double maxLon =  30.520594;

  public void testRandomObjectsSearch() {
    Random rand = new Random();
    TDoubleArrayList objects = generateObjects(1000000,rand);
    GeoHashBuilder builder = new GridGeoHashBuilder(minLat,minLon,maxLat,maxLon,250);
    GeoHashBuilder linearBuilder = new LinearGeoHashBuilder();

    TIntArrayList result = new TIntArrayList();
    for (int i = 0; i < objects.size(); i += 2) {
      double lat = objects.get(i);
      double lon = objects.get(i + 1);
      builder.add(i,lat,lon);
      linearBuilder.add(i,lat,lon);
    }

    GeoHash linearHash=  linearBuilder.build();
    GeoHash gridHash = builder.build();
    double linearTime = 0;
    double hashTime = 0;
    int iter = 1000;
    for (int i=0; i  < iter; ++i) {
      double lat = (59.829833 + (60.029015 - 59.829833) * rand.nextDouble());
      double lon = (30.178096 + (30.520594 - 30.178096) * rand.nextDouble());
      double dist = 0.5;//rand.nextDouble();
      long startTime = System.currentTimeMillis();
      long[] linearRes = linearHash.near(lat,lon,dist);
      linearTime += System.currentTimeMillis() - startTime;
      System.out.println(String.format("linear search time is %d ms", System.currentTimeMillis() - startTime));
      startTime = System.currentTimeMillis();
      long[] gridRes = gridHash.near(lat,lon,dist);
      hashTime += System.currentTimeMillis() - startTime;
      System.out.println(String.format("hash search time is %d ms", System.currentTimeMillis() - startTime));
      Arrays.sort(linearRes);
      Arrays.sort(gridRes);
      assertEquals(linearRes.length,gridRes.length);
      for (int j=0; j < linearRes.length;++j) {
        assertEquals(linearRes[j],gridRes[j]);
      }
    }
    System.out.println(String.format("mean hash search time is %f ms\n mean linear search time is %f", hashTime / iter, linearTime / iter));
  }
}
