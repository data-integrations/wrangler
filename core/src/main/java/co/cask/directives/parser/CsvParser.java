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
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.parser.Bool;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.dq.DataType;
import co.cask.wrangler.dq.TypeInference;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringEscapeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A CSV Parser Stage for parsing the {@link Row} provided based on configuration.
 */
@Plugin(type = UDD.Type)
@Name("parse-as-csv")
@Description("Parses a column as CSV (comma-separated values).")
public class CsvParser implements UDD {
  private ColumnName columnArg;
  private Text delimiterArg;
  private Bool headerArg;

  // CSV format defines the configuration for CSV parser for parsing.
  private CSVFormat format;

  //
  private boolean hasHeader;

  // Set to true once header is checked.
  private boolean checkedHeader = false;

  // Header names.
  private List<String> headers = new ArrayList<>();

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("parse-as-csv");
    builder.define("col", TokenType.COLUMN_NAME);
    builder.define("delimiter", TokenType.TEXT, Optional.TRUE);
    builder.define("header", TokenType.BOOLEAN, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    columnArg = args.value("col");

    char delimiter = ',';
    if (args.contains("delimiter")) {
      delimiterArg = args.value("delimiter");
      delimiter = delimiterArg.value().charAt(0);
      if (delimiterArg.value().startsWith("\\")) {
        String unescapedStr = StringEscapeUtils.unescapeJava(delimiterArg.value());
        if (unescapedStr == null) {
          throw new DirectiveParseException("Invalid delimiter for CSV Parser: " + delimiterArg.value());
        }
        delimiter = unescapedStr.charAt(0);
      }
    }

    this.format = CSVFormat.DEFAULT.withDelimiter(delimiter);
    this.format.withIgnoreEmptyLines(true)
      .withAllowMissingColumnNames(true)
      .withIgnoreSurroundingSpaces(true)
      .withRecordSeparator('\n');

    this.hasHeader = false;
    if(args.contains("header")) {
      headerArg = args.value("header");
      this.hasHeader = headerArg.value();
    }
  }

  /**
   * Parses a give column in a {@link Row} as a CSV Row.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context)
    throws DirectiveExecutionException {

    for (Row row : rows) {
      int idx = row.find(columnArg.value());
      if (idx == -1) {
        continue;
      }
      String line = (String) row.getValue(idx);
      if(line == null || line.isEmpty()) {
        continue;
      }
      CSVParser parser = null;
      try {
        parser = CSVParser.parse(line, format);
        List<CSVRecord> csvRecords = parser.getRecords();
        for (CSVRecord csvRecord : csvRecords) {
          if(!checkedHeader && hasHeader && isHeader(csvRecord)) {
            for (int i = 0; i < csvRecord.size(); i++) {
              headers.add(csvRecord.get(i));
            }
            if (rows.size() > 0) {
              return new ArrayList<>();
            }
          } else {
            toRow(csvRecord, row);
          }
        }
      } catch (IOException e) {
        throw new DirectiveExecutionException(
          String.format("%s : Issue parsing the row. %s", toString(), e.getMessage())
        );
      }
    }
    return rows;
  }

  /**
   * Converts a {@link CSVRecord} to {@link Row}.
   *
   * @param record
   * @return
   */
  private void toRow(CSVRecord record, Row row) {
    int size = headers.size();
    for ( int i = 0; i < record.size(); i++) {
      if (size > 0) {
        row.add(headers.get(i), record.get(i));
      } else {
        row.add(columnArg.value() + "_" + (i + 1), record.get(i));
      }
    }
  }

  private boolean isHeader(CSVRecord record) {
    checkedHeader = true;
    Set<String> columns = new HashSet<>();
    for (int i = 0; i < record.size(); i++) {
      String value = record.get(i);
      if (value == null || value.trim().isEmpty()) {
        return false;
      }
      DataType type = TypeInference.getDataType(value);
      if (type != DataType.STRING) {
        return false;
      }
      if (columns.contains(value)) {
        return false;
      } else {
        columns.add(value);
      }
    }
    return true;
  }
}
