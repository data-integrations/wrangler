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
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.httpdlog.ApacheHttpdLoglineParser;

import java.util.List;

/**
 * A Executor for parsing Apache HTTPD and NGINX log files.
 */
@Plugin(type = Directive.Type)
@Name("parse-as-log")
@Categories(categories = { "parser", "logs"})
@Description("Parses Apache HTTPD and NGINX logs.")
public class ParseLog implements Directive {
  public static final String NAME = "parse-as-log";
  private String column;
  private String format;
  private LogLine line;
  private Parser<Object> parser;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("format", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.format = ((Text) args.value("format")).value();
    this.parser = new ApacheHttpdLoglineParser<>(Object.class, format);
    this.line = new LogLine();
    List<String> paths = this.parser.getPossiblePaths();
    try {
      parser.addParseTarget(LogLine.class.getMethod("setValue", String.class, String.class), paths);
    } catch (NoSuchMethodException e) {
      // This should never happen, as the class is defined within this class.
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
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
