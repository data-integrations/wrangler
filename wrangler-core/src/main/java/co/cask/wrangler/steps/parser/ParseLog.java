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
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.httpdlog.ApacheHttpdLoglineParser;

import java.util.List;

/**
 * A Step for parsing Apache HTTPD and NGINX log files.
 */
@Usage(directive = "parse-as-log", usage = "parse-as-log <column> <format>")
public class ParseLog extends AbstractStep {
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
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws StepException In case CSV parsing generates more record.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    // Iterate through all the records.
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);

        String log;
        if (object instanceof String) {
          log = (String) object;
        } else if (object instanceof byte[]) {
          log = new String((byte[]) object);
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String or byte[].",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
        line.set(record);
        try {
          parser.parse(line, log);
        } catch (Exception e) {
          record.addOrSet("log.parse.error", 1);
        }
      }
    }
    return records;
  }

  public final class LogLine {
    private Record record;
    public void setValue(final String name, final String value) {
      record.addOrSet(name, value);
    }

    public void set(Record record) {
      this.record = record;
    }

    public Record get() {
      return record;
    }
  }

}
