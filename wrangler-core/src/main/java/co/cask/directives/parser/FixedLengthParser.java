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
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.LazyNumber;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.lineage.MutationDefinition;
import co.cask.wrangler.api.lineage.MutationType;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.NumericList;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Fixed length Parser Stage for parsing the {@link Row} provided based on configuration.
 */
@Plugin(type = Directive.Type)
@Name("parse-as-fixed-length")
@Categories(categories = { "parser"})
@Description("Parses fixed-length records using the specified widths and padding-character.")
public final class FixedLengthParser implements Directive {
  public static final String NAME = "parse-as-fixed-length";
  private int[] widths;
  private String col;
  private String padding;
  private int recordLength;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("width", TokenType.NUMERIC_LIST);
    builder.define("padding", TokenType.TEXT, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.col = ((ColumnName) args.value("column")).value();
    List<LazyNumber> numbers = ((NumericList) args.value("width")).value();
    this.widths = new int[numbers.size()];
    int idx = 0;
    int sum = 0;
    while (idx < numbers.size()) {
      this.widths[idx] = numbers.get(idx).intValue();
      sum += this.widths[idx];
      idx = idx + 1;
    }
    this.recordLength = sum;
    if (args.contains("padding")) {
      this.padding = ((Text) args.value("padding")).value();
    } else {
      this.padding = null;
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      int idx = row.find(col);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof String) {
          String data = (String) object;
          int length = data.length();
          // If the recordLength length doesn't match the string length.
          if (length < recordLength) {
            throw new ErrorRowException(
              String.format("Fewer bytes than length of row specified - expected at least %d bytes, found %s bytes.",
                            recordLength, length),
              2
            );
          }

          int index = 1;
          while ((index + recordLength - 1) <= length) {
            Row newRow = new Row(row);
            int recPosition = index;
            int colid = 1;
            for (int width : widths) {
              String val = data.substring(recPosition - 1, recPosition + width - 1);
              if (padding != null) {
                val = val.replaceAll(padding, "");
              }
              newRow.add(String.format("%s_%d", col, colid), val);
              recPosition += width;
              colid += 1;
            }
            results.add(newRow);
            index = (index + recordLength);
          }
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", col)
          );
        }
      }
    }
    return results;
  }

  @Override
  public MutationDefinition lineage() {
    MutationDefinition.Builder builder = new MutationDefinition.Builder(NAME,
      "Widths: " + Arrays.toString(widths) + ", Padding: " + padding);
    builder.addMutation(col, MutationType.READ);
    builder.addMutation("all columns formatted " + col + "_%d", MutationType.ADD);
    return builder.build();
  }
}
