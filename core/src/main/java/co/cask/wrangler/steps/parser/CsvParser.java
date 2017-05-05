/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.dq.DataType;
import co.cask.wrangler.dq.TypeInference;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A CSV Parser Stage for parsing the {@link Record} provided based on configuration.
 */
@Usage(
  directive = "parse-as-csv",
  usage = "parse-as-csv <column> <delimiter> [<header=true|false>]",
  description = "Parses a column as CSV (comma-separated values)"
)
public class CsvParser extends AbstractStep {
  // Column within the input row that needs to be parsed as CSV
  private String col;

  // CSV format defines the configuration for CSV parser for parsing.
  private CSVFormat format;

  // Replaces the input {@link Record} columns.
  private boolean hasHeader;

  // Set to true once header is checked.
  private boolean checkedHeader = false;

  // Header names.
  private List<String> headers = new ArrayList<>();

  public CsvParser(int lineno, String detail, Options options, String col, boolean hasHeader) {
    super(lineno, detail);
    this.col = col;
    this.format = CSVFormat.DEFAULT.withDelimiter(options.delimiter);
    this.format.withIgnoreEmptyLines(options.ignoreEmptyLines)
      .withAllowMissingColumnNames(options.allowMissingColumnNames)
      .withIgnoreSurroundingSpaces(options.ignoreSurroundingSpaces)
      .withRecordSeparator(options.recordSeparator);
    this.hasHeader = hasHeader;
  }

  /**
   * Parses a give column in a {@link Record} as a CSV Record.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Record containing multiple columns based on CSV parsing.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context)
    throws StepException {

    for (Record record : records) {
      int idx = record.find(col);
      if (idx == -1) {
        continue;
      }
      String line = (String) record.getValue(idx);
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
            if (records.size() > 0) {
              return new ArrayList<>();
            }
          } else {
            toRow(csvRecord, record);
          }
        }
      } catch (IOException e) {
        throw new StepException(
          String.format("%s : Issue parsing the record. %s", toString(), e.getMessage())
        );
      }
    }
    return records;
  }

  /**
   * Converts a {@link CSVRecord} to {@link Record}.
   *
   * @param record
   * @return
   */
  private void toRow(CSVRecord record, Record row) {
    int size = headers.size();
    for ( int i = 0; i < record.size(); i++) {
      if (size > 0) {
        row.add(headers.get(i), record.get(i));
      } else {
        row.add(col + "_" + (i + 1), record.get(i));
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

  /**
   * Specifies the configuration for the CSV parser.
   */
  public static class Options {
    private char delimiter = ',';
    private boolean allowMissingColumnNames = true;
    private char recordSeparator = '\n';
    private boolean ignoreSurroundingSpaces = true;
    private boolean ignoreEmptyLines = true;

    public Options() {
      // Defines the default object.
    }

    public Options(char delimiter) {
      this.delimiter = delimiter;
    }

    public Options(char delimiter, boolean ignoreEmptyLines) {
      this.delimiter = delimiter;
      this.ignoreEmptyLines = ignoreEmptyLines;
    }

    public Options (char delimiter, boolean allowMissingColumnNames, char recordSeparator,
                    boolean ignoreSurroundingSpaces, boolean ignoreEmptyLines) {
      this.delimiter = delimiter;
      this.allowMissingColumnNames = allowMissingColumnNames;
      this.recordSeparator = recordSeparator;
      this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
      this.ignoreEmptyLines = ignoreEmptyLines;
    }
  }
}
