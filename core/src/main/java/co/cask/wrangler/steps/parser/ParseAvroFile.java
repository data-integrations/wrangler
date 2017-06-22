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

package co.cask.wrangler.steps.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A step to parse AVRO File.
 */
@Plugin(type = "udd")
@Name("parse-as-avro-file")
@Usage("parse-as-avro-file <column>")
@Description("parse-as-avro-file <column>.")
public class ParseAvroFile extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(ParseAvroFile.class);
  private final String column;
  private final Gson gson;

  public ParseAvroFile(int lineno, String directive, String column) {
    super(lineno, directive);
    this.column = column;
    this.gson = new Gson();
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, final PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object instanceof byte[]) {
          DataFileReader<GenericRecord> reader = null;
          try {
            reader =
              new DataFileReader<>(new SeekableByteArrayInput((byte[]) object),
                                                new GenericDatumReader<GenericRecord>());
            while(reader.hasNext()) {
              Record newRecord = new Record();
              add(reader.next(), newRecord, null);
              results.add(newRecord);
            }
          } catch (IOException e) {
            throw new StepException(toString() + " : Failed to parse Avro data file." + e.getMessage());
          } finally {
            if (reader != null) {
              try {
                reader.close();
              } catch (IOException e) {
                // Nothing can be done.
              }
            }
          }
        } else {
          throw new StepException(toString() + " : column " + column + " should be of type byte array avro file.");
        }
      }
    }
    return results;
  }

  /**
   * Flattens the {@link GenericRecord}.
   *
   * @param genericRecord to be flattened.
   * @param record to be flattened into
   * @param name of the field to be flattened.
   */
  private void add(GenericRecord genericRecord, Record record, String name) {
    List<Schema.Field> fields = genericRecord.getSchema().getFields();
    String colname;
    for (Schema.Field field : fields) {
      Object v = genericRecord.get(field.name());
      if (name != null) {
        colname = String.format("%s_%s", name, field.name());
      } else {
        colname = field.name();
      }
      if (v instanceof GenericRecord) {
        add((GenericRecord) v, record, colname);
      } else if (v instanceof Map || v instanceof List) {
        record.add(colname, gson.toJson(v));
      } else {
        record.add(colname, genericRecord.get(field.name()));
      }
    }
  }
}
