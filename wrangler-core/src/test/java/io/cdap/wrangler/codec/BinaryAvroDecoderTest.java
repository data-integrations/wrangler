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

package io.cdap.wrangler.codec;

import io.cdap.wrangler.api.Row;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * Tests {@link BinaryAvroDecoder}
 */
public class BinaryAvroDecoderTest {

  private static final String SCHEMA = "{\"namespace\": \"example.avro\",\n" +
    " \"type\": \"record\",\n" +
    " \"name\": \"User\",\n" +
    " \"fields\": [\n" +
    "     {\"name\": \"name\", \"type\": \"string\"},\n" +
    "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
    "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
    " ]\n" +
    "}";

  @Test
  public void testBasicFunctionality() throws Exception {
    // Parse schema and validate fields.
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(SCHEMA);
    Assert.assertTrue(schema != null);
    List<Schema.Field> fields = schema.getFields();
    Assert.assertEquals(3, fields.size());

    // Create generic rows.
    GenericRecord user1  = new GenericData.Record(schema);
    user1.put("name", "Root");
    user1.put("favorite_number", 8);

    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Ben");
    user2.put("favorite_number", 7);
    user2.put("favorite_color", "red");

    // Write rows to byte array stream.
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    datumWriter.write(user1, encoder);
    datumWriter.write(user2, encoder);
    encoder.flush();
    out.close();

    byte[] bytes = out.toByteArray();

    BinaryAvroDecoder decoder = new BinaryAvroDecoder(schema);
    List<Row> rows = decoder.decode(bytes);
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals("Root", rows.get(0).getValue("name"));
    Assert.assertEquals("Ben", rows.get(1).getValue("name"));
  }
}
