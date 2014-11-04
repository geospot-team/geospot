package GeoHash;

import GeoHash.Impl.GridGeoHashBuilder;
import GeoHash.Impl.LinearGeoHashBuilder;
import GeoHash.Impl.LinearOptimizedGeoHashBuilder;
import Utils.FastScanner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;

/**
 * Created by noxoomo on 26/10/14.
 */
public class GeoHashCLI {
  public static void main(String[] args) {
    FastScanner scanner = new FastScanner(new File(args[0]));
    final int objectsCount = scanner.nextInt();

    GeoHashBuilder builder;
    if (args[2].equals("hash")) {
      double leftlat = Double.parseDouble(args[3]);
      double leftlon = Double.parseDouble(args[4]);
      double rightlat = Double.parseDouble(args[5]);
      double rightlon = Double.parseDouble(args[6]);
      int bins = Integer.parseInt(args[7]);
      builder  = new GridGeoHashBuilder(leftlat,leftlon,rightlat,rightlon,bins);
    } else {
      builder = new LinearOptimizedGeoHashBuilder();
    }
    for (int i = 0; i < objectsCount; ++i) {
      final long id = scanner.nextLong();
      final double lat = scanner.nextDouble();
      final double lon = scanner.nextDouble();
      builder.add(id, lat, lon);
    }
    final GeoHash hash = builder.build();
    final int locationCount = scanner.nextInt();
    final double distance = scanner.nextDouble();

    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(args[1]));
      writer.write("{");
      for (int i = 0; i < locationCount; ++i) {
        final double lat = scanner.nextDouble();
        final double lon = scanner.nextDouble();
        writer.write(String.format("{\"lat\" : %d, \"lon\" : %d } : ")
                + Arrays.toString(hash.near(lat, lon, distance))
                + (i + 1 < locationCount ? ",\n" : "\n"));

      }
      writer.write("}");
      writer.flush();
      writer.close();
    } catch (Exception e) {

    }
  }
}
