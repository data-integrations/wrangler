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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link JsonParser}
 */
public class JsonParserTest {

  @Test
  public void testParseJsonAndJsonPath() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body",
      "parse-as-json body_deviceReference",
      "parse-as-json body_deviceReference_OS",
      "parse-as-csv  body_deviceReference_screenSize | true",
      "drop body_deviceReference_screenSize",
      "rename body_deviceReference_screenSize_1 size1",
      "rename body_deviceReference_screenSize_2 size2",
      "rename body_deviceReference_screenSize_3 size3",
      "rename body_deviceReference_screenSize_4 size4",
      "json-path body_deviceReference_alerts signal_lost $.[*].['Signal lost']",
      "json-path signal_lost signal_lost $.[0]",
      "drop body",
      "drop body_deviceReference_OS",
      "drop body_deviceReference",
      "rename body_deviceReference_timestamp timestamp",
      "set column timestamp timestamp / 1000000",
      "drop body_deviceReference_alerts",
      "set columns timestamp,alerts,phone,battery,brand,type,comments,deviceId,os_name,os_version,size1,size2,size3,size4,signal"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "{ \"deviceReference\": { \"brand\": \"Samsung \", \"type\": \"Gear S3 frontier\", " +
        "\"deviceId\": \"SM-R760NDAAXAR\", \"timestamp\": 122121212341231, \"OS\": { \"name\": \"Tizen OS\", " +
        "\"version\": \"2.3.1\" }, \"alerts\": [ { \"Signal lost\": true }, { \"Emergency call\": true }, " +
        "{ \"Wifi connection lost\": true }, { \"Battery low\": true }, { \"Calories\": 354 } ], \"screenSize\": " +
        "\"extra-small|small|medium|large\", \"battery\": \"22%\", \"telephoneNumber\": \"+14099594986\", \"comments\": " +
        "\"It is an AT&T samung wearable device.\" } }")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 1);
  }

  @Test
  public void testArrayOfObjects() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "[ { \"a\" : 1, \"b\" : 2 }, { \"a\" : 3, \"b\" : 3 }, { \"a\" : 4, \"c\" : 5 } ]")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 3);
  }

  @Test
  public void testArrayOfNumbers() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "[1,2,3,4,5]")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 1);
  }

}