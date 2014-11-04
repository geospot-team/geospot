package GeoHash.Impl;

import GeoHash.GeoHash;
import GeoHash.GeoHashBuilder;
import gnu.trove.TDoubleArrayList;
import gnu.trove.TLongArrayList;

/**
 * Created by noxoomo on 26/10/14.
 */
//TODO:  all hacks may be wrong for all earth. but for spb and moscow  with small distance should work =)
public class Grid implements GeoHashBuilder, GeoHash {
  final double leftBoxLat;
  final double leftBoxLon;
  final double maxLat;
  final double maxLon;
  final int binsCount;
  final Bin[] bins;
  final double lonOffset;
  final double latOffset;

  Grid(double leftBoxLat, double leftBoxLon, double maxLat, double maxLon, int binsCount) {
    this.leftBoxLat = leftBoxLat;
    this.leftBoxLon = leftBoxLon;
    this.maxLat = maxLat;
    this.maxLon = maxLon;
    this.binsCount = binsCount;
    this.bins = new Bin[binsCount * binsCount + 1]; //last bin — all othere points
    for (int i = 0; i < binsCount * binsCount + 1; ++i) {
      bins[i] = new Bin();
    }
    this.latOffset = (maxLat - leftBoxLat) / binsCount;
    this.lonOffset = (maxLon - leftBoxLon) / binsCount;
  }


  private int getBin(int latBin, int lonBin) {
    return latBin * binsCount + lonBin;
  }

  @Override
  public boolean add(long id, double lat, double lon) {
    if ((lat < leftBoxLat || lat > maxLat) || (lon < leftBoxLon || lon > maxLon)) {
      return bins[binsCount * binsCount].add(id, lat, lon);
    }
    final int latBin = (int) ((lat - leftBoxLat) / (latOffset));
    final int lonBin = (int) ((lon - leftBoxLon) / (lonOffset));
    return bins[getBin(latBin, lonBin)].add(id, lat, lon);

  }

  @Override
  public GeoHash build() {
    return this;
  }


  class FilterResult {
    final int minLatBin;
    final int minLonBin;
    final int maxLatBin;
    final int maxLonBin;
    final byte[] filter;

    FilterResult(int minLatBin, int minLonBin, int maxLatBin, int maxLonBin, byte[] filter) {
      this.minLatBin = minLatBin;
      this.minLonBin = minLonBin;
      this.maxLatBin = maxLatBin;
      this.maxLonBin = maxLonBin;
      this.filter = filter;
    }
  }

  public double upperBoundDist(double lat, double lon, int bin) {
    int latBin = bin / binsCount;
    int lonBin = bin - latBin * binsCount;
    double minLatDiff = Math.min(Math.abs(lat - leftBoxLat - latBin * latOffset), Math.abs(lat - leftBoxLat - (latBin + 1) * latOffset));
    double minLonDiff = Math.min(Math.abs(lon - leftBoxLon - lonBin * lonOffset), Math.abs(lon - leftBoxLon - (lonBin + 1) * lonOffset)) * Math.PI / 180;
    final double minLonDist = GeoUtils.earthRadius * minLatDiff * Math.PI / 180;
    double minlat = Math.max(lat, leftBoxLat + latBin * latOffset) * Math.PI / 180;
    final double latCos = Math.cos(minlat);
    final double upperLatDist = GeoUtils.earthRadius * latCos * minLonDiff;
    return Math.min(upperLatDist, minLonDist);
  }

  private byte type(double lat, double lon, int bin, double dist) {
    int latBin = bin / binsCount;
    int lonBin = bin - latBin * binsCount;

    double maxLatDiff = Math.max(Math.abs(lat - leftBoxLat - latBin * latOffset), Math.abs(lat - leftBoxLat - (latBin + 1) * latOffset));
    double maxLonDiff = Math.max(Math.abs(lon - leftBoxLon - lonBin * lonOffset), Math.abs(lon - leftBoxLon - (lonBin + 1) * lonOffset)) * Math.PI / 180;
    final double maxLonDist = GeoUtils.earthRadius * maxLatDiff * Math.PI / 180;
    final double maxlat = Math.min(lat, leftBoxLat + latBin * latOffset) * Math.PI / 180;
    final double maxLatCos = Math.cos(maxlat);
    final double maxLatDist = GeoUtils.earthRadius * maxLatCos * maxLonDiff;
    if (maxLonDist * maxLonDist + maxLatDist * maxLatDist < dist * dist) {
      return (byte) 1;
    }
    double minLatDiff = Math.min(Math.abs(lat - leftBoxLat - latBin * latOffset), Math.abs(lat - leftBoxLat - (latBin + 1) * latOffset));
    double minLonDiff = Math.min(Math.abs(lon - leftBoxLon - lonBin * lonOffset), Math.abs(lon - leftBoxLon - (lonBin + 1) * lonOffset)) * Math.PI / 180;
    final double minLonDist = GeoUtils.earthRadius * minLatDiff * Math.PI / 180;
    double minlat = Math.max(lat, leftBoxLat + latBin * latOffset) * Math.PI / 180;
    final double latCos = Math.cos(minlat);
    final double upperLatDist = GeoUtils.earthRadius * latCos * minLonDiff;
    return (byte) (Math.min(upperLatDist, minLonDist) < dist ? 2 : 0);
  }

  //0 don't check
  //1 confidence
  //2 to check
  public FilterResult filterBins(double lat, double lon, double dist) {
    final int latBin = (int) ((lat - leftBoxLat) / (latOffset));
    final int lonBin = (int) ((lon - leftBoxLon) / (lonOffset));

    int minLatBin = latBin;
    int minLonBin = lonBin;
    int maxLatBin = latBin;
    int maxLonBin = lonBin;

    byte[] filter = new byte[binsCount * binsCount + 1];

    filter[binsCount * binsCount] = 2;
    filter[getBin(latBin, lonBin)] = 2;

    for (int i = latBin; i < binsCount; ++i) {
      for (int j = lonBin; j < binsCount; ++j) {
        int bin = getBin(i, j);
        final byte t = type(lat,lon,bin,dist);
        if (t != 0) {
          filter[bin] = t;
          maxLatBin = i > maxLatBin ? i : maxLatBin;
          maxLonBin = j > maxLonBin ? j : maxLonBin;
        } else {
          break;
        }
      }
      for (int j = lonBin - 1; j >= 0; --j) {
        if (j < 0)
          break;
        int bin = getBin(i, j);
        final byte t = type(lat,lon,bin,dist);
        if (t != 0) {
          filter[bin] = t;
          maxLatBin = i > maxLatBin ? i : maxLatBin;
          minLonBin = j < minLonBin ? j : minLonBin;
        } else {
          break;
        }
      }
    }

    for (int i = latBin - 1; i >= 0; --i) {
      if (i < 0)
        break;
      for (int j = lonBin; j < binsCount; ++j) {
        int bin = getBin(i, j);
        final byte t = type(lat,lon,bin,dist);
        if (t != 0) {
          filter[bin] = t;
          minLatBin = i < minLatBin ? i : minLatBin;
          maxLonBin = j > maxLonBin ? j : maxLonBin;
        } else {
          break;
        }
      }
      for (int j = lonBin - 1; j >= 0; --j) {
        if (j < 0)
          break;
        int bin = getBin(i, j);
        final byte t = type(lat,lon,bin,dist);
        if (t != 0) {
          filter[bin] = t;
          minLatBin = i < minLatBin ? i : minLatBin;
          minLonBin = j < minLonBin ? j : minLonBin;
        } else {
          break;
        }
      }
    }
    return new FilterResult(minLatBin, minLonBin, maxLatBin, maxLonBin, filter);
  }


  @Override
  public long[] near(double lat, double lon, double dist) {
    FilterResult filter = filterBins(lat, lon, dist);
    TLongArrayList ids = new TLongArrayList();
    ids.add(bins[binsCount * binsCount].near(lat, lon, dist));
    for (int latBin = filter.minLatBin; latBin <= filter.maxLatBin; ++latBin) {
      for (int lonBin = filter.minLonBin; lonBin <= filter.maxLonBin; ++lonBin) {
        final int bin = getBin(latBin, lonBin);
        if (filter.filter[bin] == 1) {
          ids.add(bins[bin].ids.toNativeArray());
        }
        if (filter.filter[bin] == 2) {
          ids.add(bins[bin].near(lat, lon, dist));
        }
      }
    }
    return ids.toNativeArray();
  }


  class Bin implements GeoHash, GeoHashBuilder {
    public final TLongArrayList ids;
    public final TDoubleArrayList coordinates;

    Bin() {
      ids = new TLongArrayList();
      coordinates = new TDoubleArrayList();
    }

    public boolean add(long id, double lat, double lon) {
      ids.add(id);
      coordinates.add(lat * Math.PI / 180);
      coordinates.add(lon * Math.PI / 180);
      return true;
    }

    @Override
    public GeoHash build() {
      return this;
    }

    @Override
    public long[] near(double destLat, double destLon, double dist) {
      destLat *= Math.PI / 180;
      destLon *= Math.PI / 180;
      TLongArrayList result = new TLongArrayList();
      for (int i = 0; i < coordinates.size(); i += 2) {
        final double lat = coordinates.get(i);
        final double lon = coordinates.get(i + 1);
        if (nearOptimized(lat, lon, destLat, destLon, dist)) {
          result.add(ids.get(i / 2));
        }
      }
      return result.toNativeArray();
    }

    private boolean nearOptimized(double lat, double lon, double destLat, double destLon, double dist) {
      final double latCos = Math.cos(lat);
      final double latSin = Math.sqrt(1 - latCos * latCos);
      final double lonDist = Math.abs(lon - destLon);
      final double destLatSin = Math.sin(destLat);
      final double destLatCos = Math.sqrt(1 - destLatSin * destLatSin);
      final double temp = latSin * destLatSin + latCos * destLatCos * Math.cos(lonDist);
      return temp >  Math.cos(dist / GeoUtils.earthRadius);
//      return (GeoUtils.earthRadius * Math.acos(temp)) < dist;
    }
  }
}
