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

import io.cdap.directives.transformation.ColumnExpression;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ColumnExpression}
 */
public class ExpressionTest {

  @Test
  public void testGeoFence() throws Exception {

    String geoJsonFence = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{}," +
        "\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-122.05870628356934,37.37943348292772]," +
        "[-122.05724716186525,37.374727268782294],[-122.04634666442871,37.37493189292912]," +
        "[-122.04608917236328,37.38175237839049],[-122.05870628356934,37.37943348292772]]]}}]}";

    String[] directives = new String[]{
        "set column result geo:InFence(lat,lon,fences)"
    };

    List<Row> rows = Arrays.asList(
        new Row("id", 123)
            .add("lon", -462.49145507812494)
            .add("lat", 43.46089378008257)
            .add("fences", geoJsonFence)
    );
    rows = TestingRig.execute(directives, rows);
    Assert.assertFalse((Boolean) rows.get(0).getValue("result"));
  }

  @Test(expected = RecipeException.class)
  public void testMalformedGeoFence() throws Exception {
    String geoJsonFence = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{}," +
        "\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-122.05870628356934,37.37943348292772]," +
        "[-122.05724716186525,37.374727268782294],[-122.04634666442871,37.37493189292912]," +
        "[-122.04608917236328,37.38175237839049]]]}}]}";

    String[] directives = new String[]{
        "set column result geo:InFence(lat,lon,fences)"
    };

    List<Row> rows = Arrays.asList(
        new Row("id", 123)
            .add("lon", -462.49145507812494)
            .add("lat", 43.46089378008257)
            .add("fences", geoJsonFence)
    );
    TestingRig.execute(directives, rows);
  }
}

