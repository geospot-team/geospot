package GeoHash.Impl;

import GeoHash.GeoHash;
import GeoHash.GeoHashBuilder;
import gnu.trove.TDoubleArrayList;
import gnu.trove.TLongArrayList;

/**
 * Created by noxoomo on 23/10/14.
 */

public class GridGeoHashBuilder implements GeoHashBuilder {
  private Grid grid;

  GridGeoHashBuilder(double minLat, double minLon, double maxLat, double maxLon, int bins) {
    this.grid = new Grid(minLat, minLon, maxLat, maxLon, bins);
  }

  @Override
  public boolean add(long id, double lat, double lon) {
    return grid.add(id,lat,lon);
  }

  @Override
  public GeoHash build() {
    return grid.build();
  }
  }
