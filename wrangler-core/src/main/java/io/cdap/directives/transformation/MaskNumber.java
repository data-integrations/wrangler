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

package io.cdap.directives.transformation;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.utils.TypeConvertor;

import java.util.List;

/**
 * A directive that applies substitution masking on the column.
 *
 * <p>
 *  Substitution masking is generally used for masking credit card or SSN numbers.
 *  This type of masking is fixed masking, where the pattern is applied on the
 *  fixed length string.
 *
 *  <ul>
 *    <li>Use of # will include the digit from the position.</li>
 *    <li>Use x/X to mask the digit at that position (converted to lowercase x in the result)</li>
 *    <li>Any other characters will be inserted as-is.</li>
 *  </ul>
 *
 *  <blockquote>
 *    <pre>
 *        Executor step = new MaskNumber(lineno, line, "ssn", "XXX-XX-####", 1);
 *        Executor step = new MaskNumber(lineno, line, "amex", "XXXX-XXXXXX-X####", 1);
 *    </pre>
 *  </blockquote>
 * </p>
 */
@Plugin(type = Directive.TYPE)
@Name(MaskNumber.NAME)
@Categories(categories = { "transform"})
@Description("Masks a column value using the specified masking pattern.")
public class MaskNumber implements Directive, Lineage {
  public static final String NAME = "mask-number";
  // Specifies types of mask
  public static final int MASK_NUMBER = 1;
  public static final int MASK_SHUFFLE = 2;

  // Masking pattern
  private String mask;

  // Column on which to apply mask.
  private String column;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("mask", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.mask = ((Text) args.value("mask")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        String value = TypeConvertor.toString(row.getValue(idx));
        if (value == null) {
          continue;
        }
        row.setValue(idx, maskNumber(value, mask));
      } else {
        row.add(column, new String(""));
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Masked numbers in the column '%s' using masking pattern '%s'", column, mask)
      .relation(column, column)
      .build();
  }

  private String maskNumber(String number, String mask) {
    int index = 0;
    StringBuilder masked = new StringBuilder();
    for (int i = 0; i < mask.length(); i++) {
      char c = mask.charAt(i);
      if (c == '#') {
        // if we have print numbers and the mask index has exceed, we continue further.
        if (index > number.length() - 1) {
          continue;
        }
        masked.append(number.charAt(index));
        index++;
      } else if (c == 'x' || c == 'X') {
        masked.append(Character.toLowerCase(c));
        index++;
      } else {
        if (index < number.length()) {
          char c1 = number.charAt(index);
          if (c1 == c) {
            index++;
          }
        }
        masked.append(c);
      }
    }
    return masked.toString();
  }
}


