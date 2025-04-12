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

package io.cdap.wrangler.utils;

import com.google.common.base.Charsets;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.RemoteDirectiveResponse;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link ObjectSerDe}
 */
public class ObjectSerDeTest {

  @Test
  public void testSerDe() throws Exception {
    ObjectSerDe<List<Row>> objectSerDe = new ObjectSerDe<>();
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("bytes", "foo".getBytes(Charsets.UTF_8)).add("a", 1).add("b", 2.0));
    rows.add(new Row("bytes", "boo".getBytes(Charsets.UTF_8)).add("a", 2).add("b", 3.0));
    byte[] bytes = objectSerDe.toByteArray(rows);
    List<Row> newRows = objectSerDe.toObject(bytes);
    Assert.assertEquals(rows.size(), newRows.size());
    Assert.assertEquals(rows.get(0).getColumn(0), newRows.get(0).getColumn(0));
    Assert.assertEquals(rows.get(0).getColumn(1), newRows.get(0).getColumn(1));
    Assert.assertEquals(rows.get(0).getColumn(2), newRows.get(0).getColumn(2));
  }

  @Test
  public void testNull() throws Exception {
    ObjectSerDe<List<Row>> objectSerDe = new ObjectSerDe<>();
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("bytes", null));
    rows.add(new Row("bytes", null));
    byte[] bytes = objectSerDe.toByteArray(rows);
    List<Row> newRows = objectSerDe.toObject(bytes);
    Assert.assertEquals(rows.size(), newRows.size());
  }

  @Test
  public void testLogicalTypeSerDe() throws Exception {
    ObjectSerDe<List<Row>> objectSerDe = new ObjectSerDe<>();
    List<Row> expectedRows = new ArrayList<>();

    Row firstRow = new Row();
    firstRow.add("id", 1);
    firstRow.add("name", "abc");
    firstRow.add("date", LocalDate.of(2018, 11, 11));
    firstRow.add("time", LocalTime.of(11, 11, 11));
    firstRow.add("timestamp", ZonedDateTime.of(2018, 11 , 11 , 11, 11, 11, 0, ZoneId.of("UTC")));
    expectedRows.add(firstRow);
    byte[] bytes = objectSerDe.toByteArray(expectedRows);
    List<Row> actualRows = objectSerDe.toObject(bytes);
    Assert.assertEquals(expectedRows.size(), actualRows.size());

    Row secondRow = new Row();
    secondRow.add("id", 2);
    secondRow.add("name", null);
    secondRow.add("date", LocalDate.of(2018, 12, 11));
    secondRow.add("time", LocalTime.of(11, 12, 11));
    secondRow.add("timestamp", null);
    expectedRows.add(secondRow);
    bytes = objectSerDe.toByteArray(expectedRows);
    actualRows = objectSerDe.toObject(bytes);
    Assert.assertEquals(expectedRows.size(), actualRows.size());
  }
  @Test
  public void testRemoteDirectiveResponseSerDe() throws Exception {
    List<Row> expectedRows = new ArrayList<>();
    Row firstRow = new Row();
    firstRow.add("id", 1);
    expectedRows.add(firstRow);
    Schema expectedSchema = Schema.recordOf(Schema.Field.of("id", Schema.of(Schema.Type.INT)));
    RemoteDirectiveResponse expectedResponse = new RemoteDirectiveResponse(expectedRows, expectedSchema);
    ObjectSerDe<RemoteDirectiveResponse> objectSerDe = new ObjectSerDe<>();

    byte[] bytes = objectSerDe.toByteArray(expectedResponse);
    RemoteDirectiveResponse actualResponse = objectSerDe.toObject(bytes);

    Assert.assertEquals(expectedResponse.getRows().size(), actualResponse.getRows().size());
    Assert.assertEquals(expectedResponse.getOutputSchema(), actualResponse.getOutputSchema());
  }
}
