package GeoHash;

/**
 * Created by noxoomo on 23/10/14.
 */
public interface GeoHashBuilder {
  boolean add(long id, double lat, double lon);
  GeoHash build();
}
