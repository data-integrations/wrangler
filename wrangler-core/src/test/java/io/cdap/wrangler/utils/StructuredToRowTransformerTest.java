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
 *
 */

package io.cdap.wrangler.utils;

import com.google.common.base.Charsets;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
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

/**
 * Structured to row transform test
 */
public class StructuredToRowTransformerTest {
  @Test
  public void testStructuredRecordToRow() {
    Schema schema = Schema.recordOf(
      "schema",
      Schema.Field.of("f1", Schema.of(Schema.Type.INT)),
      Schema.Field.of("f2", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("f3", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("f4", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("f5", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("f6", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("f7", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("f8", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("f9", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("f10", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
      Schema.Field.of("f11", Schema.of(Schema.LogicalType.TIME_MICROS)),
      Schema.Field.of("f12", Schema.of(Schema.LogicalType.TIME_MILLIS)),
      Schema.Field.of("f13", Schema.decimalOf(3, 2)),
      Schema.Field.of("f14", Schema.of(Schema.LogicalType.DATETIME)));

    StructuredRecord record =
      StructuredRecord.builder(schema)
        .set("f1", 1).set("f2", "aaa").set("f3", 1L).set("f4", 0d)
        .set("f5", ByteBuffer.wrap("test".getBytes(Charsets.UTF_8)))
        .set("f6", true).set("f7", 0f).setDate("f8", LocalDate.now()).setTimestamp("f9", ZonedDateTime.now())
        .setTimestamp("f10", ZonedDateTime.now()).setTime("f11", LocalTime.now()).setTime("f12", LocalTime.now())
        .set("f13", ByteBuffer.wrap(new BigDecimal(new BigInteger("111"), 2).unscaledValue().toByteArray()))
        .setDateTime("f14", LocalDateTime.now())
        .build();

    Row row = StructuredToRowTransformer.transform(record);

    // assert the byte field is byte array
    Assert.assertTrue(row.getValue("f5") instanceof byte[]);
    Assert.assertArrayEquals("test".getBytes(Charsets.UTF_8), (byte[]) row.getValue("f5"));
    // set it to byte buffer to compare all values
    row.addOrSet("f5", ByteBuffer.wrap((byte[]) row.getValue("f5")));

    Row expected = new Row();
    expected.add("f1", 1);
    expected.add("f2", "aaa");
    expected.add("f3", 1L);
    expected.add("f4", 0d);
    expected.add("f5", ByteBuffer.wrap("test".getBytes(Charsets.UTF_8)));
    expected.add("f6", true);
    expected.add("f7", 0f);
    expected.add("f8", record.getDate("f8"));
    expected.add("f9", record.getTimestamp("f9"));
    expected.add("f10", record.getTimestamp("f10"));
    expected.add("f11", record.getTime("f11"));
    expected.add("f12", record.getTime("f12"));
    expected.add("f13", record.getDecimal("f13"));
    expected.add("f14", record.getDateTime("f14"));
    Assert.assertEquals(expected, row);
  }
}
