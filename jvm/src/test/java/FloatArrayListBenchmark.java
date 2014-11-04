import gnu.trove.TFloatArrayList;
import gnu.trove.TIntArrayList;
import junit.framework.TestCase;

import java.util.Random;

/**
 * Created by noxoomo on 25/09/14.
 */
public class FloatArrayListBenchmark extends TestCase {
  public static final double earthRadius = 6378;
  public static final float destLat = (float) (59.829833f * Math.PI / 180);
  public static final float destLon = (float) (30.178096f * Math.PI / 180);

  public static boolean near(float lat, float lon, float dist) {
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

  public static boolean nearOptimized(float lat, float lon, float dist) {
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
    return (float) (earthRadius * Math.acos(temp)) < dist;
  }

  public static TFloatArrayList generateObjects(int size, Random rand) {
    TFloatArrayList objects = new TFloatArrayList(size * 2);
    for (int i = 0; i < size; ++i) {
      float lat = (float) (59.829833 + (60.029015 - 59.829833) * rand.nextDouble());
      float lon = (float) (30.178096 + (30.520594 - 30.178096) * rand.nextDouble());
      objects.add(lat);
      objects.add(lon);
    }
    return objects;
  }

  public TIntArrayList findNear(float dist, TFloatArrayList objects) {
    TIntArrayList result = new TIntArrayList();
    for (int i = 0; i < objects.size(); i += 2) {
      float lat = objects.get(i);
      float lon = objects.get(i + 1);
      if (nearOptimized(lat, lon, dist)) {
        result.add(i / 2);
      }
    }
    return result;
  }


  public void checkNear(float dist, TFloatArrayList objects) {
    TIntArrayList result = new TIntArrayList();
    for (int i = 0; i < objects.size(); i += 2) {
      float lat = objects.get(i);
      float lon = objects.get(i + 1);
      if (near(lat, lon, dist) != nearOptimized(lat, lon, dist)) {
        near(lat, lon, dist);
        nearOptimized(lat, lon, dist);
      }
      assertEquals(near(lat, lon, dist), nearOptimized(lat, lon, dist));
    }
  }

  public void testLinearSearchBenchmark() {
    Random rand = new Random();

    for (int degree = 3; degree < 10; ++degree) {
      double meanTime = 0;
      int N = 100;
      for (int iter = 0; iter < N; ++iter) {
        TFloatArrayList objects = generateObjects((int) Math.pow(10, degree), rand);
        long startTime = System.currentTimeMillis();
        findNear(1f, objects);
        meanTime += System.currentTimeMillis() - startTime;
      }
      meanTime /= N;
      System.out.println(String.format("For degree 10^%d linear search mean time is %f ms", degree, meanTime));
    }
  }

  public void testNear() {
    Random rand = new Random();

    for (int degree = 3; degree < 6; ++degree) {
      int N = 100;
      for (int iter = 0; iter < N; ++iter) {
        TFloatArrayList objects = generateObjects((int) Math.pow(10, degree), rand);
        checkNear(1.f, objects);
      }
    }
  }

}
