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

package io.cdap.directives.transformation;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link TextMetricMeasure}
 */
public class TextMetricMeasureTest {
  private static final List<Row> ROWS = Arrays.asList(
    // Correct Row.
    new Row("string1", "This is an example for distance measure.").add("string2", "This test is made of works that " +
      "are similar. This is an example for distance measure."),

    // Row that has one string empty.
    new Row("string1", "This is an example for distance measure.").add("string2", ""),

    // Row that has one string as different type.
    new Row("string1", "This is an example for distance measure.").add("string2", 1L),

    // Row that has only one column.
    new Row("string1", "This is an example for distance measure.")
  );


  @Test
  public void testTextDistanceMeasure() throws Exception {
    String[] directives = new String[] {
      "text-metric cosine string1 string2 cosine",
      "text-metric euclidean string1 string2 euclidean",
      "text-metric block-distance string1 string2 block_distance",
      "text-metric identity string1 string2 identity",
      "text-metric block string1 string2 block",
      "text-metric dice string1 string2 dice",
      "text-metric jaro string1 string2 jaro",
      "text-metric longest-common-subsequence string1 string2 lcs1",
      "text-metric longest-common-substring string1 string2 lcs2",
      "text-metric overlap-cofficient string1 string2 oc",
      "text-metric damerau-levenshtein string1 string2 dl",
      "text-metric simon-white string1 string2 sw",
      "text-metric levenshtein string1 string2 levenshtein",
    };

    List<Row> results = TestingRig.execute(directives, ROWS);
    Assert.assertTrue(results.size() == 4);
    Assert.assertEquals(15, results.get(0).width());
    Assert.assertEquals(15, results.get(1).width());
    Assert.assertEquals(15, results.get(2).width());
    Assert.assertEquals(14, results.get(3).width());
  }
}
