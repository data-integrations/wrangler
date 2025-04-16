/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.wrangler.service.directive;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;

public class WranglerDisplaySerializerTest {

  @Test
  public void testRowSerialization() {
    Gson gson = new GsonBuilder().registerTypeAdapterFactory(new WranglerDisplaySerializer()).create();
    Row subRow = new Row();
    subRow.add("f1", 1);
    subRow.add("f2", "aaa");
    subRow.add("f3", 1L);
    subRow.add("f4", 0d);
    subRow.add("f5", "test".getBytes(Charsets.UTF_8));
    subRow.add("f6", true);
    subRow.add("f7", 0f);
    subRow.add("f8", LocalDate.now());
    subRow.add("f9", ZonedDateTime.now());
    subRow.add("f10", ZonedDateTime.now());
    subRow.add("f11", LocalTime.now());
    subRow.add("f12", new BigDecimal(new BigInteger("111"), 2));
    subRow.add("f13", LocalDateTime.now());
    subRow.add("f14", Arrays.asList("A", "B", "C"));
    Row rowToConvert = new Row(subRow);
    rowToConvert.add("f15", subRow);
    String serializedOutput = gson.toJson(rowToConvert);

    String f1Str = "1";
    String f2Str = "\"aaa\"";
    String f3Str = "1";
    String f4Str = "0.0";
    String f5Str = "\"" + WranglerDisplaySerializer.NONDISPLAYABLE_STRING + "\"";
    String f6Str = "true";
    String f7Str = "0.0";
    String f8Str = "\"" + subRow.getValue("f8").toString() + "\"";
    String f9Str = "\"" + subRow.getValue("f9").toString() + "\"";
    String f10Str = "\"" + subRow.getValue("f10").toString() + "\"";
    String f11Str = "\"" + subRow.getValue("f11").toString() + "\"";
    String f12Str = "\"1.11\"";
    String f13Str = "\"" + subRow.getValue("f13").toString() + "\"";
    String f14Str = "[\"A\",\"B\",\"C\"]";
    String f15Str = String.format("{\"f1\":%s,\"f2\":%s,\"f3\":%s,\"f4\":%s,\"f5\":%s,\"f6\":%s,\"f7\":%s,\"f8\":%s,"
      + "\"f9\":%s,\"f10\":%s,\"f11\":%s,\"f12\":%s,\"f13\":%s,\"f14\":%s}", f1Str, f2Str, f3Str, f4Str, f5Str, f6Str,
                                  f7Str, f8Str, f9Str, f10Str, f11Str, f12Str, f13Str, f14Str);

    String outputTest = String.format("{\"f1\":%s,\"f2\":%s,\"f3\":%s,\"f4\":%s,\"f5\":%s,\"f6\":%s,\"f7\":%s,"
      + "\"f8\":%s,\"f9\":%s,\"f10\":%s,\"f11\":%s,\"f12\":%s,\"f13\":%s,\"f14\":%s,\"f15\":%s}", f1Str, f2Str, f3Str,
                                      f4Str, f5Str, f6Str, f7Str, f8Str, f9Str, f10Str, f11Str, f12Str, f13Str, f14Str,
                                      f15Str);
    Assert.assertEquals(serializedOutput, outputTest);
  }

}
