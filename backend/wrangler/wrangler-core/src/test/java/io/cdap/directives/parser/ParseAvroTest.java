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

package io.cdap.directives.parser;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.codec.JsonAvroDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test {@link ParseAvro}
 */
public class ParseAvroTest {

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
  public void testAvroBinaryRecordReadWrite() throws Exception {
    // Parse schema and validate fields.
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(SCHEMA);
    Assert.assertTrue(schema != null);
    List<Schema.Field> fields = schema.getFields();
    Assert.assertEquals(3, fields.size());

    // Create generic records.
    GenericRecord user1  = new GenericData.Record(schema);
    user1.put("name", "Root");
    user1.put("favorite_number", 8);
    user1.put("favorite_color", "blue");

    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Ben");
    user2.put("favorite_number", 7);
    user2.put("favorite_color", "red");

    // Write records to byte array stream.
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    datumWriter.write(user1, encoder);
    datumWriter.write(user2, encoder);
    encoder.flush();
    out.close();

    byte[] serializedBytes = out.toByteArray();

    // Read from byte array.
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    ByteArrayInputStream in = new ByteArrayInputStream(serializedBytes);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
    List<GenericRecord> records = new ArrayList<>();
    while (true) {
      try {
        GenericRecord record = datumReader.read(null, decoder);
        records.add(record);
      } catch (EOFException e) {
        break;
      }
    }
    Assert.assertEquals(2, records.size());
  }

  @Test
  public void testAvroJsonRecordReadWrite() throws Exception {
    Schema schema = getSchema();
    byte[] bytes = encodeAsJsonGenericRecord();
    JsonAvroDecoder jsonAvroDecoder = new JsonAvroDecoder(schema);
    List<Row> r = jsonAvroDecoder.decode(bytes);
    Assert.assertEquals(2, r.size());

    // Read from byte array.
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, in);
    List<GenericRecord> records = new ArrayList<>();
    while (true) {
      try {
        GenericRecord record = datumReader.read(null, decoder);
        records.add(record);
      } catch (EOFException e) {
        break;
      }
    }
    in.close();
    Assert.assertEquals(2, records.size());
  }

  private Schema getSchema() throws Exception {
    // Parse schema and validate fields.
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(SCHEMA);
    Assert.assertTrue(schema != null);
    List<Schema.Field> fields = schema.getFields();
    Assert.assertEquals(3, fields.size());
    return schema;
  }

  private byte[] encodeAsJsonGenericRecord() throws Exception {
    Schema schema = getSchema();

    // Create generic records.
    GenericRecord user1  = new GenericData.Record(schema);
    user1.put("name", "Root");
    user1.put("favorite_number", 8);

    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Ben");
    user2.put("favorite_number", 7);
    user2.put("favorite_color", "red");

    // Write records to byte array stream.
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
    datumWriter.write(user1, encoder);
    datumWriter.write(user2, encoder);
    encoder.flush();
    out.close();

    return out.toByteArray();
  }
}
