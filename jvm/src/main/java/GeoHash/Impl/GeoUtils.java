package GeoHash.Impl;

import GeoHash.GeoHash;

/**
 * Created by noxoomo on 26/10/14.
 */

public class GeoUtils {
  static double earthRadius = 6378;

  public static double distance(double lat, double lon, double destLat, double destLon) {
    final double latCos = Math.cos(lat);
    final double latSin = Math.sqrt(1 - latCos * latCos);
    final double lonDist = Math.abs(lon - destLon);
    final double destLatSin = Math.sin(destLat);
    final double destLatCos = Math.sqrt(1 - destLatSin * destLatSin);
    final double temp = latSin * destLatSin + latCos * destLatCos * Math.cos(lonDist);
    return (earthRadius * Math.acos(temp));
  }
}
