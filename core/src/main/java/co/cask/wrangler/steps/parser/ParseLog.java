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
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.annotations.Usage;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.httpdlog.ApacheHttpdLoglineParser;

import java.util.List;

/**
 * A Directive for parsing Apache HTTPD and NGINX log files.
 */
@Plugin(type = "udd")
@Name("parse-as-log")
@Usage("parse-as-log <column> <format>")
@Description("Parses Apache HTTPD and NGINX logs.")
public class ParseLog extends AbstractDirective {
  private final String column;
  private final String format;
  private final LogLine line;
  private final Parser<Object> parser;

  public ParseLog(int lineno, String detail, String column, String format) {
    super(lineno, detail);
    this.column = column;
    this.format = format;
    this.line = new LogLine();
    this.parser = new ApacheHttpdLoglineParser<>(Object.class, format);
    List<String> paths = this.parser.getPossiblePaths();
    try {
      parser.addParseTarget(LogLine.class.getMethod("setValue", String.class, String.class), paths);
    } catch (NoSuchMethodException e) {
      // This should never happen, as the class is defined within this class.
    }
  }

  /**
   * Parses a give column value based on the format specified.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws DirectiveExecutionException In case CSV parsing generates more row.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    // Iterate through all the rows.
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);

        String log;
        if (object instanceof String) {
          log = (String) object;
        } else if (object instanceof byte[]) {
          log = new String((byte[]) object);
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String or byte[].",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
        line.set(row);
        try {
          parser.parse(line, log);
        } catch (Exception e) {
          row.addOrSet("log.parse.error", 1);
        }
      }
    }
    return rows;
  }

  public final class LogLine {
    private Row row;

    public void setValue(final String name, final String value) {
      String key = name.toLowerCase();
      if (key.contains("original") || key.contains("bytesclf") || key.contains("cookie") ) {
        return;
      }
      key = key.replaceAll("[^a-zA-Z0-9_]","_");
      row.addOrSet(key, value);
    }

    public void set(Row row) {
      this.row = row;
    }

    public Row get() {
      return row;
    }
  }

}
