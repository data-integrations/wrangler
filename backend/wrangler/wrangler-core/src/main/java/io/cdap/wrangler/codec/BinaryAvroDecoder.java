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

import com.google.gson.Gson;
import io.cdap.wrangler.api.Row;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class {@link BinaryAvroDecoder} decodes a byte array of AVRO Json Records into the {@link Row} structure.
 */
public class BinaryAvroDecoder extends AbstractAvroDecoder {
  private final Gson gson;

  public BinaryAvroDecoder(Schema schema) {
    super(schema);
    this.gson = new Gson();
  }

  /**
   * Decodes byte array of binary encoded AVRO into list of {@link Row}.
   * This method will iterate through each of the AVRO schema fields and translate
   * them into columns within the {@link Row}.
   *
   * If the field is instance of {@link List} or {@link Map} it is converted into JSON
   * representation. In order to flatten or expand such columns other directives need
   * to be used.
   *
   * @param bytes array of bytes that contains binary encoded AVRO record.
   * @return list of {@link Row} that are converted from AVRO encoded binary messages.
   */
  @Override
  public List<Row> decode(byte[] bytes) throws DecoderException {
    List<Row> rows = new ArrayList<>();
    BinaryDecoder decoder = null;
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    try {
      decoder = DecoderFactory.get().binaryDecoder(in, null);
      while (true) {
        try {
          GenericRecord gRecord = getReader().read(null, decoder);
          List<Schema.Field> fields = getSchema().getFields();
          Row r = new Row();
          for (Schema.Field field : fields) {
            Object object = gRecord.get(field.name());
            if (object instanceof Utf8) {
              Utf8 o = (Utf8) object;
              object = o.toString();
            } else if (object instanceof Map || object instanceof List) {
              object = gson.toJson(object);
            }
            r.add(field.name(), object);
          }
          rows.add(r);
        } catch (EOFException e) {
          break; // Reached end of buffer.
        }
      }
    } catch (AvroTypeException e) {
      throw new DecoderException(e.getMessage());
    } catch (IOException e) {
      throw new DecoderException("Issue creating AVRO binary decoder. Verify the schema.");
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        // Can't do anything.
      }
    }
    return rows;
  }
}
