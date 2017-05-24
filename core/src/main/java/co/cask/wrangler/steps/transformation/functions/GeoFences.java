/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.steps.transformation.functions;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.model.Coordinates;
import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Polygon;
import com.github.filosganga.geogson.model.positions.SinglePosition;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import java.util.List;

/**
 * GeoFencing check based on location and polygon
 */
public final class GeoFences {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapterFactory(new GeometryAdapterFactory())
      .create();

  /**
   * Static method to be used with jexl
   * Checks if Point is inside any of the given polygonal geofences based on the winding number algorithm.
   *
   * @param latitude  latitude of the location to verify
   * @param longitude longitude of the location to verify
   * @param geofences GeoJson representation of the fence area
   * @return true if location is inside any of the given geofences, else false
   */
  public static Boolean inFence(double latitude, double longitude, String geofences) {
    Coordinates location = Coordinates.of(longitude, latitude);
    Boolean inzone = false;
    FeatureCollection featureCollection;
    try {
      featureCollection = GSON.fromJson(geofences, FeatureCollection.class);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("String %s is not a valid geoJson representation of fence",
          geofences), e);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException(String.format("String %s is not a valid Json string", geofences), e);
    }
    for (Feature feature : featureCollection.features()) {
      inzone = inzone || isPointInside(feature, location);
    }
    return inzone;
  }

  private static Boolean isPointInside(Feature feature, Coordinates location) {
    Polygon polygon = (Polygon) feature.geometry();
    List<SinglePosition> positions = Lists.newArrayList(polygon.perimeter().positions().children());

    if ((positions == null) || (location == null)) {
      return false;
    }

    int wn = 0;
    for (int i = 0; i < positions.size() - 1; i++) {
      if (positions.get(i).coordinates().getLat() <= location.getLat()) {
        if (positions.get(i + 1).coordinates().getLat() > location.getLat()) {
          if (isLeft(positions.get(i).coordinates(), positions.get(i + 1).coordinates(), location) > 0.0) {
            ++wn;
          }
        }
      } else {
        if (positions.get(i + 1).coordinates().getLat() <= location.getLat()) {
          if (isLeft(positions.get(i).coordinates(), positions.get(i + 1).coordinates(), location) < 0.0) {
            --wn;
          }
        }
      }
    }
    return (wn != 0);
  }

  private static double isLeft(Coordinates vertex0, Coordinates vertex1, Coordinates gpC) {
    return (vertex1.getLon() - vertex0.getLon()) * (gpC.getLat() - vertex0.getLat()) -
        (gpC.getLon() - vertex0.getLon()) * (vertex1.getLat() - vertex0.getLat());
  }
}
