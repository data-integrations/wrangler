/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.functions;

import org.junit.Assert;
import org.junit.Test;


/**
 * Tests {@link GeoFences}
 */
public class GeoFenceTest {

  @Test
  public void testWithMultipleFences() {

    String geoJsonFence = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{}," +
        "\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-122.05870628356934,37.37943348292772]," +
        "[-122.05724716186525,37.374727268782294],[-122.04634666442871,37.37493189292912]," +
        "[-122.04608917236328,37.38175237839049],[-122.05870628356934,37.37943348292772]]]}}," +
        "{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":" +
        "[[[-122.05055236816405,37.36862239166385],[-122.04038143157959,37.36841775030572]," +
        "[-122.04141139984132,37.37312436031927],[-122.05055236816405,37.36862239166385]]]}}]}";

    Assert.assertFalse(GeoFences.InFence(43.46089378008257, -462.49145507812494, geoJsonFence));
    Assert.assertTrue(GeoFences.InFence(37.378990156513105, -122.05076694488525, geoJsonFence));
  }

  @Test
  public void testWithSingleFence() {
    String geoJsonFence = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{}," +
        "\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-122.05870628356934,37.37943348292772]," +
        "[-122.05724716186525,37.374727268782294],[-122.04634666442871,37.37493189292912]," +
        "[-122.04608917236328,37.38175237839049],[-122.05870628356934,37.37943348292772]]]}}]}";

    Assert.assertFalse(GeoFences.InFence(43.46089378008257, -462.49145507812494, geoJsonFence));
    Assert.assertTrue(GeoFences.InFence(37.378990156513105, -122.05076694488525, geoJsonFence));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithOpenPolygon() {

    String geoJsonFence = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{},\"geometry\"" +
        ":{\"type\":\"Polygon\",\"coordinates\":[[[-122.05870628356934,37.37943348292772]," +
        "[-122.05724716186525,37.374727268782294],[-122.04634666442871,37.37493189292912]," +
        "[-122.04608917236328,37.38175237839049]]]}}]}";

    GeoFences.InFence(43.46089378008257, -462.49145507812494, geoJsonFence);
  }
}
