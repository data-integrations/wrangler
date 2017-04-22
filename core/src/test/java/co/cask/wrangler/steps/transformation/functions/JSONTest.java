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

import co.cask.wrangler.utils.JsonTestData;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link JSON}
 */
public class JSONTest {
  @Test
  public void testLowercaseKeys() throws Exception {
    JsonParser parser = new JsonParser();
    JsonElement lower = JSON.keysToLower(parser.parse(JsonTestData.JSON_ARRAY_WITH_OBJECT_CASE_MIX));
    JsonElement expected = JSON.parse(JsonTestData.JSON_ARRAY_WITH_OBJECT);
    Assert.assertTrue(expected.equals(lower));
  }
}
