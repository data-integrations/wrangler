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
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
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
public class ParseAvroFile extends AbstractDirective {
  private static final Logger LOG = LoggerFactory.getLogger(ParseAvroFile.class);
  private final String column;
  private final Gson gson;

  public ParseAvroFile(int lineno, String directive, String column) {
    super(lineno, directive);
    this.column = column;
    this.gson = new Gson();
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows  Input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, final RecipeContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof byte[]) {
          DataFileReader<GenericRecord> reader = null;
          try {
            reader =
              new DataFileReader<>(new SeekableByteArrayInput((byte[]) object),
                                                new GenericDatumReader<GenericRecord>());
            while(reader.hasNext()) {
              Row newRow = new Row();
              add(reader.next(), newRow, null);
              results.add(newRow);
            }
          } catch (IOException e) {
            throw new DirectiveExecutionException(toString() + " : Failed to parse Avro data file." + e.getMessage());
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
          throw new DirectiveExecutionException(toString() + " : column " + column + " should be of type byte array avro file.");
        }
      }
    }
    return results;
  }

  /**
   * Flattens the {@link GenericRecord}.
   *
   * @param genericRecord to be flattened.
   * @param row to be flattened into
   * @param name of the field to be flattened.
   */
  private void add(GenericRecord genericRecord, Row row, String name) {
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
        add((GenericRecord) v, row, colname);
      } else if (v instanceof Map || v instanceof List) {
        row.add(colname, gson.toJson(v));
      } else {
        row.add(colname, genericRecord.get(field.name()));
      }
    }
  }
}
