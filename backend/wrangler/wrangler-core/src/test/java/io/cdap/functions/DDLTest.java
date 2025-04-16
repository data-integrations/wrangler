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

package io.cdap.functions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests {@link DDL}
 */
public class DDLTest {
  @Test
  public void testGetRecursiveRecord() {
    Schema inner2 = Schema.recordOf("inner2",
                                    Schema.Field.of("x", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("y", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("z", Schema.arrayOf(Schema.of(Schema.Type.INT))));
    Schema inner1 = Schema.recordOf("inner1",
                                    Schema.Field.of("rec2", Schema.arrayOf(Schema.nullableOf(inner2))),
                                    Schema.Field.of("s", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("l", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("m", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                      Schema.of(Schema.Type.INT))));
    Schema nestedSchema = Schema.recordOf(
      "nested",
      Schema.Field.of("rec1", inner1),
      Schema.Field.of("z", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("a", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("m", Schema.mapOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)),
                                        Schema.nullableOf(inner2))));

    StructuredRecord record2 = StructuredRecord.builder(inner2)
      .set("x", "str2")
      .set("y", 5)
      .set("z", ImmutableList.of(0, 1, 2, 3))
      .build();
    StructuredRecord record1 = StructuredRecord.builder(inner1)
      .set("s", "str1")
      .set("l", 3L)
      .set("rec2", Lists.newArrayList(null, record2))
      .set("m", ImmutableMap.of("a", 1, "b", 2))
      .build();
    StructuredRecord nestedRecord = StructuredRecord.builder(nestedSchema)
      .set("rec1", record1)
      .set("z", true)
      .set("a", ImmutableList.of("a", "b", "c"))
      .set("m", ImmutableMap.of("rec2", record2))
      .build();

    Assert.assertEquals(inner1, DDL.select(nestedSchema, "rec1"));
    Assert.assertEquals(record1, DDL.select(nestedRecord, "rec1"));

    Assert.assertEquals(Schema.of(Schema.Type.BOOLEAN), DDL.select(nestedSchema, "z"));
    Assert.assertTrue((boolean) DDL.select(nestedRecord, "z"));

    Assert.assertEquals(Schema.arrayOf(Schema.of(Schema.Type.STRING)), DDL.select(nestedSchema, "a"));

    Assert.assertEquals(Schema.mapOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)),
                                     Schema.nullableOf(inner2)),
                        DDL.select(nestedSchema, "m"));
    Assert.assertEquals(nestedRecord.<Map<String, StructuredRecord>>get("m"), DDL.select(nestedRecord, "m"));
    Assert.assertEquals(Schema.nullableOf(inner2), DDL.select(nestedSchema, "m[rec2]"));

    Assert.assertEquals(Schema.of(Schema.Type.STRING), DDL.select(nestedSchema, "a[0]"));
    Assert.assertEquals("a", DDL.select(nestedRecord, "a[0]"));
    Assert.assertEquals("b", DDL.select(nestedRecord, "a[1]"));
    Assert.assertEquals("c", DDL.select(nestedRecord, "a[2]"));

    Assert.assertEquals(Schema.of(Schema.Type.STRING), DDL.select(nestedSchema, "rec1.s"));
    Assert.assertEquals("str1", DDL.select(nestedRecord, "rec1.s"));

    Assert.assertEquals(Schema.of(Schema.Type.LONG), DDL.select(nestedSchema, "rec1.l"));
    Assert.assertEquals(3L, DDL.<Long>select(nestedRecord, "rec1.l").longValue());

    Assert.assertEquals(Schema.arrayOf(Schema.nullableOf(inner2)), DDL.select(nestedSchema, "rec1.rec2"));

    Assert.assertEquals(Schema.mapOf(Schema.of(Schema.Type.STRING),
                                     Schema.of(Schema.Type.INT)),
                        DDL.select(nestedSchema, "rec1.m"));
    Assert.assertEquals(Schema.of(Schema.Type.INT), DDL.select(nestedSchema, "rec1.m[a]"));
    Assert.assertEquals(1, DDL.<Integer>select(nestedRecord, "rec1.m[a]").intValue());
    Assert.assertEquals(2, DDL.<Integer>select(nestedRecord, "rec1.m[b]").intValue());

    Assert.assertEquals(Schema.nullableOf(inner2), DDL.select(nestedSchema, "rec1.rec2[0]"));
    Assert.assertNull(DDL.select(nestedRecord, "rec1.rec2[0]"));
    Assert.assertEquals(record2, DDL.select(nestedRecord, "rec1.rec2[1]"));
    Assert.assertEquals(record2, DDL.select(nestedRecord, "m[rec2]"));

    Assert.assertEquals(Schema.of(Schema.Type.STRING), DDL.select(nestedSchema, "rec1.rec2[1].x"));
    Assert.assertEquals(Schema.of(Schema.Type.STRING), DDL.select(nestedSchema, "m[rec2].x"));
    Assert.assertEquals("str2", DDL.select(nestedRecord, "rec1.rec2[1].x"));
    Assert.assertEquals("str2", DDL.select(nestedRecord, "m[rec2].x"));

    Assert.assertEquals(Schema.of(Schema.Type.INT), DDL.select(nestedSchema, "rec1.rec2[1].y"));
    Assert.assertEquals(Schema.of(Schema.Type.INT), DDL.select(nestedSchema, "m[rec2].y"));
    Assert.assertEquals(5, DDL.<Integer>select(nestedRecord, "rec1.rec2[1].y").intValue());
    Assert.assertEquals(5, DDL.<Integer>select(nestedRecord, "m[rec2].y").intValue());

    Assert.assertEquals(Schema.arrayOf(Schema.of(Schema.Type.INT)), DDL.select(nestedSchema, "rec1.rec2[1].z"));
    Assert.assertEquals(Schema.arrayOf(Schema.of(Schema.Type.INT)), DDL.select(nestedSchema, "m[rec2].z"));

    Assert.assertEquals(Schema.of(Schema.Type.INT), DDL.select(nestedSchema, "rec1.rec2[1].z[0]"));
    Assert.assertEquals(Schema.of(Schema.Type.INT), DDL.select(nestedSchema, "m[rec2].z[0]"));
    Assert.assertEquals(0, DDL.<Integer>select(nestedRecord, "rec1.rec2[1].z[0]").intValue());
    Assert.assertEquals(1, DDL.<Integer>select(nestedRecord, "rec1.rec2[1].z[1]").intValue());
    Assert.assertEquals(2, DDL.<Integer>select(nestedRecord, "rec1.rec2[1].z[2]").intValue());
    Assert.assertEquals(3, DDL.<Integer>select(nestedRecord, "rec1.rec2[1].z[3]").intValue());
    Assert.assertEquals(0, DDL.<Integer>select(nestedRecord, "m[rec2].z[0]").intValue());
    Assert.assertEquals(1, DDL.<Integer>select(nestedRecord, "m[rec2].z[1]").intValue());
    Assert.assertEquals(2, DDL.<Integer>select(nestedRecord, "m[rec2].z[2]").intValue());
    Assert.assertEquals(3, DDL.<Integer>select(nestedRecord, "m[rec2].z[3]").intValue());

    Schema newSchema = DDL.drop(nestedSchema, "rec1.rec2[0].z");
    newSchema = DDL.drop(newSchema, "rec1.rec2[0].x");
    newSchema = DDL.drop(newSchema, "rec1.m");
    newSchema = DDL.drop(newSchema, "rec1.l");
    newSchema = DDL.drop(newSchema, "m");
    newSchema = DDL.drop(newSchema, "a");

    Schema inner2New = Schema.recordOf("inner2",
                                       Schema.Field.of("y", Schema.of(Schema.Type.INT)));
    Schema inner1New = Schema.recordOf("inner1",
                                       Schema.Field.of("rec2", Schema.arrayOf(Schema.nullableOf(inner2New))),
                                       Schema.Field.of("s", Schema.of(Schema.Type.STRING)));
    Schema nestedSchemaNew = Schema.recordOf(
      "nested",
      Schema.Field.of("rec1", inner1New),
      Schema.Field.of("z", Schema.of(Schema.Type.BOOLEAN)));
    Assert.assertEquals(nestedSchemaNew, newSchema);
  }
}
