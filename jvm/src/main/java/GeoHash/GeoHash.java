package GeoHash;

/**
 * Created by noxoomo on 23/10/14.
 */
public interface GeoHash {
  long[] near(double lat, double lon, double dist);
}
