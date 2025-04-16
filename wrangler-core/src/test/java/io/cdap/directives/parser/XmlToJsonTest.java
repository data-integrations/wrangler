/*
 *  Copyright © 2024 Cask Data, Inc.
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

package io.cdap.directives.parser;

import io.cdap.directives.xml.XmlToJson;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link XmlToJson}
 */
public class XmlToJsonTest {
  @Test
  public void testAutoConversionOfStringField() throws Exception {
    String[] directives = new String[] {
      "copy body body_1 true",
      "copy body body_2 true",
      "copy body body_3 true",
      "parse-xml-to-json body_1 1",
      "parse-xml-to-json body_2 1 false",
      "parse-xml-to-json body_3 1 true"
    };

    List<Row> rows = Arrays.asList(
      new Row("body",
              "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><Data><tagid>303246306303E8</tagid></Data>")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("{\"tagid\":3.03246306303E19}", rows.get(0).getValue("body_1_Data").toString());
    Assert.assertEquals("{\"tagid\":3.03246306303E19}", rows.get(0).getValue("body_2_Data").toString());
    Assert.assertEquals("{\"tagid\":\"303246306303E8\"}", rows.get(0).getValue("body_3_Data").toString());
  }
}
