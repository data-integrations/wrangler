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

package co.cask.wrangler.steps.writer;

import co.cask.wrangler.api.ParseDirectives;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.SimpleTextDirectives;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link WriteAsJsonObject}.
 */
public class WriteAsJsonObjectTest {
  private static final String EVENT = "  {\n" +
    "  \t\"fname\" : \"root\",\n" +
    "  \t\"lname\" : \"joltie\",\n" +
    "  \t\"age\" : 28,\n" +
    "  \t\"height\" : 5.9,\n" +
    "  \t\"weight\" : 178,\n" +
    "  \t\"address\" : \"Super Mars, Mars Ave, Mars, 8999\",\n" +
    "  \t\"latitude\" : -122.43345423,\n" +
    "  \t\"longitude\" : 37.234424223\n" +
    "  }";

  @Test
  public void testCreatingJSONObject() throws Exception {
    String[] recipe = new String[] {
      "parse-as-json event",
      "columns-replace s/event_//",
      "write-as-json-object coordinates latitude,longitude",
      "keep coordinates",
      "write-as-json-map location",
      "keep location"
    };

    List<Record> records = Arrays.asList(
      new Record("event", EVENT)
    );

    ParseDirectives directives = new SimpleTextDirectives(recipe);
    RecipePipelineExecutor executor = new RecipePipelineExecutor();
    executor.configure(directives, null);
    records = executor.execute(records);

    Assert.assertEquals(1, records.size());
    Assert.assertEquals("{\"coordinates\":{\"latitude\":-122.43345423,\"longitude\":37.234424223}}",
                        records.get(0).getValue("location"));
  }

}