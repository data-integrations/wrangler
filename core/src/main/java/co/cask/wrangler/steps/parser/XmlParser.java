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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Usage;
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
@Plugin(type = "udd")
@Name("parse-as-xml")
@Usage("parse-as-xml <column>")
@Description("Parses a column as XML.")
public class XmlParser extends AbstractStep {
  // Column within the input row that needs to be parsed as CSV
  private String col;
  private final VTDGen vg = new VTDGen();


  public XmlParser(int lineno, String detail, String col) {
    super(lineno, detail);
    this.col = col;
  }

  /**
   * Parses a give column in a {@link Record} as a XML.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Record containing multiple columns based on CSV parsing.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context)
    throws DirectiveExecutionException {

    for (Record record : records) {
      int idx = record.find(col);
      if (idx == -1) {
        continue; // didn't find the column.
      }

      Object object = record.getValue(idx);
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
        record.setValue(idx, vn);
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
    for ( int i = 0; i < record.size(); i++) {
      row.add(col + "_" + (i + 1), record.get(i));
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
