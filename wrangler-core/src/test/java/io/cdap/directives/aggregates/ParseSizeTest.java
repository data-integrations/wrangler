/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.directives.aggregates;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;

/**
 * Tests parsing of byte size values
 */
public class ParseSizeTest {

  @Test
  public void testParseMB() throws Exception {
    // Create test data with MB values
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("size", "1MB"));
    rows.add(new Row("size", "2MB"));
    rows.add(new Row("size", "0.5MB"));

    // Define recipe to parse sizes
    String[] recipe = new String[]{
      "parse-size size parsed_size"
    };

    // Execute the recipe
    rows = TestingRig.execute(recipe, rows);

    // Verify the parsing results
    Assert.assertEquals(3, rows.size());
    
    // 1MB = 1,048,576 bytes
    Assert.assertEquals(1048576.0, (double) rows.get(0).getValue("parsed_size"), 0.001);
    
    // 2MB = 2,097,152 bytes
    Assert.assertEquals(2097152.0, (double) rows.get(1).getValue("parsed_size"), 0.001);
    
    // 0.5MB = 524,288 bytes
    Assert.assertEquals(524288.0, (double) rows.get(2).getValue("parsed_size"), 0.001);
  }
} 