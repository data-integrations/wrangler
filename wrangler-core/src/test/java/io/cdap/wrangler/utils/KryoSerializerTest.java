/*
 * Copyright © 2024 Cask Data, Inc.
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
package io.cdap.wrangler.utils;

import com.google.common.collect.Lists;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.RemoteDirectiveResponse;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KryoSerializerTest {

  private static final String[] TESTS = new String[]{
      JsonTestData.BASIC,
      JsonTestData.SIMPLE_JSON_OBJECT,
      JsonTestData.ARRAY_OF_OBJECTS,
      JsonTestData.JSON_ARRAY_WITH_OBJECT,
      JsonTestData.COMPLEX_1,
      JsonTestData.ARRAY_OF_NUMBERS,
      JsonTestData.ARRAY_OF_STRING,
      JsonTestData.COMPLEX_2,
      JsonTestData.EMPTY_OBJECT,
      JsonTestData.NULL_OBJECT,
      JsonTestData.FB_JSON
  };

  private static final String[] directives = new String[]{
      "set-column body json:Parse(body)"
  };

  @Test
  public void testJsonTypes() throws Exception {
    SchemaConverter converter = new SchemaConverter();
    RecordConvertor recordConvertor = new RecordConvertor();
    JsonParser parser = new JsonParser();
    RecipePipeline executor = TestingRig.execute(directives);
    for (String test : TESTS) {
      Row row = new Row("body", test);

      List<Row> expectedRows = executor.execute(Lists.newArrayList(row));
      byte[] serializedRows = new KryoSerializer().fromRemoteDirectiveResponse(
          new RemoteDirectiveResponse(expectedRows, null));
      List<Row> gotRows = new KryoSerializer().toRemoteDirectiveResponse(serializedRows).getRows();
      Assert.assertArrayEquals(expectedRows.toArray(), gotRows.toArray());
    }
  }

  @Test
  public void testLogicalTypes() throws Exception {
    Row testRow = new Row();
    testRow.add("id", 1);
    testRow.add("name", "abc");
    testRow.add("date", LocalDate.of(2018, 11, 11));
    testRow.add("time", LocalTime.of(11, 11, 11));
    testRow.add("timestamp", ZonedDateTime.of(2018, 11, 11, 11, 11, 11, 0, ZoneId.of("UTC")));
    testRow.add("bigdecimal", new BigDecimal(new BigInteger("123456"), 5));
    testRow.add("datetime", LocalDateTime.now());
    List<Row> expectedRows = Collections.singletonList(testRow);
    byte[] serializedRows = new KryoSerializer().fromRemoteDirectiveResponse(
        new RemoteDirectiveResponse(expectedRows, null));
    List<Row> gotRows = new KryoSerializer().toRemoteDirectiveResponse(serializedRows).getRows();
    Assert.assertArrayEquals(expectedRows.toArray(), gotRows.toArray());
  }

  @Test
  public void testCollectionTypes() throws Exception {
    List<Integer> list = new ArrayList<>();
    list.add(null);
    list.add(1);
    list.add(2);
    Set<Integer> set = new HashSet<>();
    set.add(null);
    set.add(1);
    set.add(2);
    Map<String, Integer> map = new HashMap<>();
    map.put("null", null);
    map.put("1", 1);
    map.put("2", 2);

    Row testRow = new Row();
    testRow.add("list", list);
    testRow.add("set", set);
    testRow.add("map", map);

    List<Row> expectedRows = Collections.singletonList(testRow);
    byte[] serializedRows = new KryoSerializer().fromRemoteDirectiveResponse(
        new RemoteDirectiveResponse(expectedRows, null));
    List<Row> gotRows = new KryoSerializer().toRemoteDirectiveResponse(serializedRows).getRows();
    Assert.assertArrayEquals(expectedRows.toArray(), gotRows.toArray());
  }

  @Test
  public void testWithSchema() throws Exception {
    Row testRow = new Row();
    testRow.add("id", 1);
    testRow.add("name", "abc");
    testRow.add("date", LocalDate.of(2018, 11, 11));
    testRow.add("time", LocalTime.of(11, 11, 11));
    testRow.add("timestamp", ZonedDateTime.of(2018, 11, 11, 11, 11, 11, 0, ZoneId.of("UTC")));
    testRow.add("bigdecimal", new BigDecimal(new BigInteger("123456"), 5));
    testRow.add("datetime", LocalDateTime.now());
    List<Row> expectedRows = Collections.singletonList(testRow);

    SchemaConverter converter = new SchemaConverter();
    Schema expectedSchema = converter.toSchema("myrecord", expectedRows.get(0));

    byte[] serializedRows = new KryoSerializer().fromRemoteDirectiveResponse(
        new RemoteDirectiveResponse(expectedRows, expectedSchema));
    RemoteDirectiveResponse response = new KryoSerializer().toRemoteDirectiveResponse(
        serializedRows);

    Assert.assertArrayEquals(expectedRows.toArray(), response.getRows().toArray());
    Assert.assertEquals(expectedSchema, response.getOutputSchema());
  }
}
