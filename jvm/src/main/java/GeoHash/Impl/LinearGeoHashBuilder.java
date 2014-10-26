package GeoHash.Impl;
import GeoHash.GeoHash;
import GeoHash.GeoHashBuilder;
import gnu.trove.TDoubleArrayList;
import gnu.trove.TLongArrayList;

/**
 * Created by noxoomo on 23/10/14.
 */

public class LinearGeoHashBuilder implements GeoHashBuilder {
  private TDoubleArrayList coordinates = new TDoubleArrayList();
  private TLongArrayList ids = new TLongArrayList();
  @Override
  public boolean add(long id, double lat, double lon) {
    coordinates.add(lat* Math.PI / 180);
    coordinates.add(lon * Math.PI / 180);
    ids.add(id);
    return true;
  }

  @Override
  public GeoHash build() {
    return new LinearHash(coordinates.toNativeArray(),ids.toNativeArray());
  }

  private static class LinearHash implements GeoHash {
    final double earthRadius = 6378;
    final double[] coordinates;
    final long[] ids;
    LinearHash(double[] coordinates, long[] ids) {
      this.coordinates = coordinates;
      this.ids = ids;
    }

    @Override
    public long[] near(double destLat, double destLon, double dist) {
      destLat *= Math.PI / 180;
      destLon *= Math.PI / 180;
      TLongArrayList result = new TLongArrayList();
      for (int i = 0; i < coordinates.length; i += 2) {
        final double lat = coordinates[i];
        final  double lon = coordinates[i + 1];
        if (nearOptimized(lat, lon, destLat,destLon,dist )) {
          result.add(ids[i / 2]);
        }
      }
      return result.toNativeArray();
    }

    private  boolean nearOptimized(double lat, double lon, double destLat, double destLon, double dist) {
      //optimization hacks
      final double lowerLonDist = 110.567 * 180 / Math.PI * Math.abs(lat - destLat);
      if (lowerLonDist > dist)
        return false;
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
      return  (earthRadius * Math.acos(temp)) < dist;
    }
  }

}
