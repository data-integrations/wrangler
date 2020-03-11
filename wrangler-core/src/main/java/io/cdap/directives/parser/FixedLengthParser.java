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
import io.cdap.wrangler.api.LazyNumber;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.NumericList;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A Fixed length Parser Stage for parsing the {@link Row} provided based on configuration.
 */
@Plugin(type = Directive.TYPE)
@Name("parse-as-fixed-length")
@Categories(categories = { "parser"})
@Description("Parses fixed-length records using the specified widths and padding-character.")
public final class FixedLengthParser implements Directive, Lineage {
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

        if (object == null) {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' has null value. It should be a non-null 'String'.", col));
        }

        if (object instanceof String) {
          String data = (String) object;
          int length = data.length();
          // If the recordLength length doesn't match the string length.
          if (length < recordLength) {
            throw new ErrorRowException(
              NAME, String.format("Column '%s' contains a value with fewer characters than the specified length " +
                                    "of row. Expected at least %d characters but found %s characters.",
                                  col, recordLength, length), 2);
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
            NAME, String.format("Column '%s' has invalid type '%s'. It should be of type 'String'.",
                                col, object.getClass().getSimpleName()));
        }
      }
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Parsed column '%s' with fixed lengths for columns", col)
      .all(Many.of(col), Many.of(col))
      .build();
  }
}
