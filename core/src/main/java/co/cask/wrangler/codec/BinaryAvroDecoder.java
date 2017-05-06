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

package co.cask.wrangler.codec;

import co.cask.wrangler.api.DecoderException;
import co.cask.wrangler.api.Record;
import com.google.common.base.Charsets;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class {@link BinaryAvroDecoder} decodes a byte array of AVRO Json Records into the {@link Record} structure.
 */
public class BinaryAvroDecoder extends AbstractAvroDecoder {
  private final DatumWriter<GenericRecord> writer;
  private final ByteArrayOutputStream out;

  public BinaryAvroDecoder(Schema schema) {
    super(schema);
    writer = new GenericDatumWriter<>(schema);
    out = new ByteArrayOutputStream();
  }

  @Override
  public List<Record> decode(byte[] bytes, String column) throws DecoderException {
    List<Record> records = new ArrayList<>();
    BinaryDecoder decoder = null;
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    try {
      decoder = DecoderFactory.get().binaryDecoder(in, null);
      while (true) {
        try {
          out.reset();
          JsonEncoder encoder = EncoderFactory.get().jsonEncoder(getSchema(), out);
          GenericRecord gRecord = getReader().read(null, decoder);
          writer.write(gRecord, encoder);
          encoder.flush();
          out.close();
          records.add(new Record(column, new String(out.toByteArray(), Charsets.UTF_8)));
        } catch (EOFException e) {
          break; // Reached end of buffer.
        }
      }
    } catch (AvroTypeException e) {
      throw new DecoderException(e.getMessage());
    } catch (IOException e) {
      throw new DecoderException("Issue create json decoder, verify the schema");
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        // Can't do anything.
      }
    }
    return records;
  }
}
