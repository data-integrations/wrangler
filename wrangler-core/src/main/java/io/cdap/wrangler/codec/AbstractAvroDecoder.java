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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 * This class {@link AbstractAvroDecoder} is implementation of {@link Decoder} interface using type {@link Row}.
 * All implementations of AVRO decoder should extend from this class.
 */
public abstract class AbstractAvroDecoder implements Decoder<Row> {
  // Schema associated with record or data file being read.
  private final Schema schema;

  // Reader for reading data based on the schema.
  private final DatumReader<GenericRecord> reader;

  protected AbstractAvroDecoder(Schema schema) {
    this.schema = schema;
    this.reader = new GenericDatumReader<>(this.schema);
  }

  protected Schema getSchema() {
    return schema;
  }

  protected DatumReader<GenericRecord> getReader() {
    return reader;
  }
}
