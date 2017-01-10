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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.util.List;

/**
 * A CSV Parser Stage for parsing the {@link Row} provided based on configuration.
 */
public class CsvParser extends AbstractStep {
  // Column within the input row that needs to be parsed as CSV
  private String col;

  // CSV format defines the configuration for CSV parser for parsing.
  private CSVFormat format;

  // Replaces the input {@link Row} columns.
  boolean replaceColumns;

  public CsvParser(int lineno, String detail, Options options, String col, boolean replaceColumns) {
    super(lineno, detail);
    this.col = col;
    this.format = CSVFormat.newFormat(options.delimiter);
    this.format.withIgnoreEmptyLines(options.ignoreEmptyLines)
      .withAllowMissingColumnNames(options.allowMissingColumnNames)
      .withIgnoreSurroundingSpaces(options.ignoreSurroundingSpaces)
      .withRecordSeparator(options.recordSeparator);
    this.replaceColumns = replaceColumns;
  }

  /**
   * Parses a give column in a {@link Row} as a CSV Record.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws StepException In case CSV parsing generates more record.
   */
  @Override
  public Row execute(Row row) throws StepException, SkipRowException {
    String line = (String) row.getValue(col);
    if (line == null) {
      throw new StepException(toString() + " : Did not find " + col + " in the row");
    }
    CSVParser parser = null;
    try {
      parser = CSVParser.parse(line, format);
      List<CSVRecord> records = parser.getRecords();
      if (records.size() > 1) {
        throw new StepException(toString() + " : " + this.getClass().getSimpleName() +
                                         "generated more that 1 record as output. Interface doesn't support");
      }
      return toRow(records.get(0), row);
    } catch (IOException e) {
      throw new StepException(toString(), e);
    }
  }

  /**
   * Converts a {@link CSVRecord} to {@link Row}.
   *
   * @param record
   * @return
   */
  private Row toRow(CSVRecord record, Row row) {
    if (replaceColumns) {
      row = new Row();
    }

    int start = row.length();
    for ( int i = 0; i < record.size(); i++) {
      row.addColumn("col" + (start + i));
      row.addValue(record.get(i));
    }
    return row;
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
