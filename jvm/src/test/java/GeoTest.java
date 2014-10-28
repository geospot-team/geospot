import gnu.trove.TDoubleArrayList;
import junit.framework.TestCase;

import java.util.Random;

/**
 * Created by noxoomo on 26/10/14.
 */
public abstract class GeoTest extends TestCase {
  public static final double earthRadius = 6378;
  public static final double destLat =  (59.829833f * Math.PI / 180);
  public static final double destLon = (30.178096f * Math.PI / 180);

  public static boolean near(double lat, double lon, double dist) {
    lat *= Math.PI / 180;
    lon *= Math.PI / 180;
    final double lonDist = Math.abs(lon - destLon);
    final double latSin = Math.sin(lat);
    final double latCos = Math.sqrt(1 - latSin * latSin);
    final double destLatSin = Math.sin(destLat);
    final double destLatCos = Math.sqrt(1 - destLatSin * destLatSin);
    final double temp = latSin * destLatSin + latCos * destLatCos * Math.cos(lonDist);
    return (earthRadius * Math.acos(temp) < dist);
  }

  public static boolean nearOptimized(double lat, double lon, double dist) {
    //optimization hacks
    final double lowerLonDist = 110.567 * Math.abs(lat - destLat * 180 / Math.PI);
    if (lowerLonDist > dist)
      return false;
    lat *= Math.PI / 180;
    lon *= Math.PI / 180;
    final double latCos = Math.cos(lat);
    final double lowerLatDist = earthRadius * latCos * Math.abs(lon - destLon);
    if (lowerLatDist > dist)
      return false;
    if (lowerLatDist * lowerLatDist + lowerLonDist * lowerLonDist > dist * dist)
      return false;
    final double latSin = Math.sqrt(1 - latCos * latCos);
    final double lonDist = Math.abs(lon - destLon);
    final double destLatSin = Math.sin(destLat);
    final double destLatCos = Math.sqrt(1 - destLatSin * destLatSin);
    final double temp = latSin * destLatSin + latCos * destLatCos * Math.cos(lonDist);
    return (earthRadius * Math.acos(temp)) < dist;
  }

  public static TDoubleArrayList generateObjects(int size, Random rand) {
    TDoubleArrayList objects = new TDoubleArrayList(size * 2);
    for (int i = 0; i < size; ++i) {
      double lat = (59.829833 + (60.029015 - 59.829833) * rand.nextDouble());
      double lon =  (30.178096 + (30.520594 - 30.178096) * rand.nextDouble());
      objects.add(lat);
      objects.add(lon);
    }
    return objects;
  }
}
