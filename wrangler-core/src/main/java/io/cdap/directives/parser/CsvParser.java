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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.Bool;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.dq.DataType;
import io.cdap.wrangler.dq.TypeInference;
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
@Plugin(type = Directive.TYPE)
@Name(CsvParser.NAME)
@Categories(categories = { "parser", "csv"})
@Description("Parses a column as CSV (comma-separated values).")
public class CsvParser implements Directive, Lineage {
  public static final String NAME = "parse-as-csv";
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
          throw new DirectiveParseException(
            NAME, String.format("Invalid delimiter for CSV Parser '%s'", delimiterArg.value()));
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
    if (args.contains("header")) {
      headerArg = args.value("header");
      this.hasHeader = headerArg.value();
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  /**
   * Parses a give column in a {@link Row} as a CSV Row.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   */
  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {

    for (Row row : rows) {
      int idx = row.find(columnArg.value());
      if (idx == -1) {
        continue;
      }
      String line = (String) row.getValue(idx);
      if (line == null || line.isEmpty()) {
        continue;
      }
      CSVParser parser = null;
      try {
        parser = CSVParser.parse(line, format);
        List<CSVRecord> csvRecords = parser.getRecords();
        for (CSVRecord csvRecord : csvRecords) {
          if (!checkedHeader && hasHeader && isHeader(csvRecord)) {
            for (int i = 0; i < csvRecord.size(); i++) {
              headers.add(csvRecord.get(i).trim().replaceAll("\\s+", "_"));
            }
            if (rows.size() > 0) {
              return new ArrayList<>();
            }
          } else {
            toRow(csvRecord, row);
          }
        }
      } catch (IOException e) {
        // When there is error parsing data, the data is written to error.
        throw new ErrorRowException(NAME, e.getMessage(), 1);
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
    for (int i = 0; i < record.size(); i++) {
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

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Parsed column '%s' as CSV with delimiter '%s'", columnArg.value(), delimiterArg.value())
      .all(Many.columns(columnArg), Many.columns(columnArg))
      .build();
  }
}
