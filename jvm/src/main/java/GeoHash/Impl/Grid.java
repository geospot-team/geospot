package GeoHash.Impl;

import GeoHash.GeoHash;
import GeoHash.GeoHashBuilder;
import gnu.trove.TDoubleArrayList;
import gnu.trove.TLongArrayList;

/**
 * Created by noxoomo on 26/10/14.
 */
public class Grid implements GeoHashBuilder, GeoHash {
  final double minLat;
  final double minLon;
  final double maxLat;
  final double maxLon;
  final int binsCount;
  final Bin[] bins;
  final double lonOffset;
  final double latOffset;

  Grid(double minLat, double minLon, double maxLat, double maxLon, int binsCount) {
    this.minLat = minLat;
    this.minLon = minLon;
    this.maxLat = maxLat;
    this.maxLon = maxLon;
    this.binsCount = binsCount;
    this.bins = new Bin[binsCount * binsCount + 1]; //last bin — all othere points
    this.latOffset = (maxLat - minLat) / binsCount;
    this.lonOffset = (maxLon - minLon) / binsCount;
  }


  private int getBin(int latBin, int lonBin) {

    return latBin * binsCount + lonBin;
  }

  @Override
  public boolean add(long id, double lat, double lon) {
    if ((lat < minLat || lat > maxLat) || (lon < minLon || lon > maxLon)) {
      return bins[binsCount * binsCount].add(id, lat, lon);
    }

    final int latBin = (int) ((lat - minLat) / (latOffset));
    final int lonBin = (int) ((lon - minLat) / (lonOffset));
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

  //0 don't check
  //1 confidence
  //2 to check
  public FilterResult filterBins(double lat, double lon, double dist) {
    final int latBin = (int) ((lat - minLat) / (latOffset));
    final int lonBin = (int) ((lon - minLat) / (lonOffset));

    int minLatBin = binsCount;
    int minLonBin = binsCount;
    int maxLatBin = -1;
    int maxLonBin = -1;

    byte[] filter = new byte[binsCount * binsCount + 1];

    filter[binsCount * binsCount + 1] = 2;
    filter[getBin(latBin, lonBin)] = 2;

    for (int i = latBin + 1; i < binsCount; ++i) {
      for (int j = lonBin + 1; j < binsCount; ++j) {
        double maxLon = lon + lonOffset * (j + 1);
        double maxLat = lat + latOffset + (i + 1);
        if (GeoUtils.distance(lat, lon, maxLat, maxLon) < dist) {
          filter[getBin(i, j)] = 2;
          maxLatBin = i > maxLatBin ? i : maxLatBin;
          maxLonBin = j > maxLonBin ? j : maxLonBin;
        } else {
          break;
        }
      }
      for (int j = lonBin - 1; j > 0; --j) {
        double minLon = lon + lonOffset * i;
        double maxLat = lat + latOffset + (i + 1);
        if (GeoUtils.distance(lat, lon, maxLat, minLon) < dist) {
          filter[getBin(i, j)] = 2;
          maxLatBin = i > maxLatBin ? i : maxLatBin;
          minLonBin = j < minLonBin ? j : minLonBin;
        } else {
          break;
        }
      }
    }

    for (int i = latBin - 1; i > 0; --i) {
      for (int j = lonBin + 1; j < binsCount; ++j) {
        double maxLon = lon + lonOffset * (j + 1);
        double minLat = lat + latOffset + i;
        if (GeoUtils.distance(lat, lon, minLat, maxLon) < dist) {
          filter[getBin(i, j)] = 2;
          minLatBin = i < minLatBin ? i : minLatBin;
          maxLonBin = j > maxLonBin ? j : maxLonBin;

        } else {
          break;
        }
      }
      for (int j = lonBin - 1; j > 0; --j) {
        double minLon = lon + lonOffset * j;
        double minLat = lat + latOffset + i;
        if (GeoUtils.distance(lat, lon, minLat, minLon) < dist) {
          filter[getBin(i, j)] = 2;
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
      return (GeoUtils.earthRadius * Math.acos(temp)) < dist;
    }
  }
}
