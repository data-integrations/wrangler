/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A step to parse AVRO File.
 */
@Plugin(type = Directive.Type)
@Name("parse-as-avro-file")
@Categories(categories = { "parser", "avro"})
@Description("parse-as-avro-file <column>.")
public class ParseAvroFile implements Directive {
  public static final String NAME = "parse-as-avro-file";
  private String column;
  private Gson gson;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    gson = new Gson();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, final ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof byte[]) {
          DataFileReader<GenericRecord> reader = null;
          try {
            reader =
              new DataFileReader<>(new SeekableByteArrayInput((byte[]) object), new GenericDatumReader<>());
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
          throw new DirectiveExecutionException(toString() + " : column " + column +
                                                  " should be of type byte array avro file.");
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
      } else if (v instanceof Utf8) {
        row.add(colname, v.toString());
      } else {
        row.add(colname, genericRecord.get(field.name()));
      }
    }
  }
}
