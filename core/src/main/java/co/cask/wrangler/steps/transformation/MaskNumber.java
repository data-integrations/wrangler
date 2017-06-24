/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.utils.TypeConvertor;

import java.util.List;

/**
 * A Wrangler plugin that applies substitution masking on the column.
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
 *        Directive step = new MaskNumber(lineno, line, "ssn", "XXX-XX-####", 1);
 *        Directive step = new MaskNumber(lineno, line, "amex", "XXXX-XXXXXX-X####", 1);
 *    </pre>
 *  </blockquote>
 * </p>
 */
@Plugin(type = "udd")
@Name("mask-number")
@Usage("mask-number <column> <pattern>")
@Description("Masks a column value using the specified masking pattern.")
public class MaskNumber extends AbstractDirective {
  // Specifies types of mask
  public static final int MASK_NUMBER = 1;
  public static final int MASK_SHUFFLE = 2;

  // Masking pattern
  private final String mask;

  // Column on which to apply mask.
  private final String column;

  public MaskNumber(int lineno, String detail, String column, String mask) {
    super(lineno, detail);
    this.mask = mask;
    this.column = column;
  }

  /**
   * Masks the column specified using either substitution method.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Record} with masked column.
   * @throws DirectiveExecutionException thrown when there is issue with masking
   */
  @Override
  public List<Record> execute(List<Record> records, RecipeContext context) throws DirectiveExecutionException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        String value = TypeConvertor.toString(record.getValue(idx));
        if (value == null) {
          continue;
        }
        record.setValue(idx, maskNumber(value, mask));
      } else {
        record.add(column, new String(""));
      }
    }
    return records;
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


