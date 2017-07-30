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

package co.cask.directives.xml;

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
import com.ximpleware.ParseException;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import org.apache.commons.csv.CSVRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A XML Parser.
 *
 * <p>
 *   TODO: This code has to be moved out into a plugin due to VTDNav once we have
 *   the plugin framework.
 * </p>
 */
@Plugin(type = Directive.Type)
@Name("parse-as-xml")
@Categories(categories = { "xml"})
@Description("Parses a column as XML.")
public class XmlParser implements Directive {
  public static final String NAME = "parse-as-xml";
  // Column within the input row that needs to be parsed as CSV
  private String column;
  private final VTDGen vg = new VTDGen();

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException {

    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        continue; // didn't find the column.
      }

      Object object = row.getValue(idx);
      if (object == null) {
        continue; // If it's null keep it as null.
      }

      if (object instanceof String) {
        String xml = (String) object;
        vg.setDoc(xml.getBytes(StandardCharsets.UTF_8));
        try {
          vg.parse(true);
        } catch (ParseException e) {
          e.printStackTrace();
        }
        VTDNav vn = vg.getNav();
        row.setValue(idx, vn);
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
    for ( int i = 0; i < record.size(); i++) {
      row.add(column + "_" + (i + 1), record.get(i));
    }
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
