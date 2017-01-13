/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;

import java.util.Random;

/**
 * A Wrangler plugin that applies substitution and shuffling masking on the column.
 *
 * <p>
 *  Substitution masking us generally used for masking credit card or SSN numbers.
 *  This type of masking is fixed masking, where the pattern is applied on the
 *  fixed length string.
 *
 *  <ul>
 *    <li>Use of # will include the digit from the position.</li>
 *    <li>Use x to mask the digit at that position</li>
 *    <li>Any other characters will be inserted as-is.</li>
 *  </ul>
 *
 *  <blockquote>
 *    <pre>
 *        Step step = new Mask(lineno, line, "ssn", "XXX-XX-####", 1);
 *        Step step = new Mask(lineno, line, "amex", "XXXX-XXXXXX-X####", 1);
 *    </pre>
 *  </blockquote>
 * </p>
 *
 * <p>
 *   Fixed length shuffle masking performs obfuscation by using random character
 *   substitution method. The data is randomly shuffled in the column.
 *
 *   <blockquote>
 *     <pre>
 *       Step step = new Mask(lineno, line, "150 Mars Avenue, Marcity, Mares", 2);
 *     </pre>
 *   </blockquote>
 * </p>
 */
public class Mask extends AbstractStep {
  // Specifies types of mask
  public static final int MASK_NUMBER = 1;
  public static final int MASK_SHUFFLE = 2;

  // Masking pattern
  private final String mask;

  // Column on which to apply mask.
  private final String column;

  // Type of mask.
  private final int maskType;

  public Mask(int lineno, String detail, String column, int maskType) {
    super(lineno, detail);
    this.mask = "";
    this.column = column;
    this.maskType = maskType;
  }

  public Mask(int lineno, String detail, String column, String mask, int maskType) {
    super(lineno, detail);
    this.mask = mask;
    this.column = column;
    this.maskType = maskType;
  }

  /**
   * Masks the column specified using either substitution method or shuffling.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Row} with masked column.
   * @throws StepException thrown when there is issue with masking
   * @throws SkipRowException thrown when the row needs to be skipped
   */
  @Override
  public Row execute(Row row, PipelineContext context) throws StepException, SkipRowException {
    Row masked = new Row(row);
    int idx = row.find(column);
    if (idx != -1) {
      if (maskType == MASK_NUMBER) {
        try {
          masked.setValue(idx, maskNumber((String)row.getValue(idx), mask));
        } catch (Exception e) {
          System.out.println(e.getMessage());
        }
      } else if (maskType == MASK_SHUFFLE) {
        masked.setValue(idx, maskShuffle((String)row.getValue(idx), 0));
      }
    } else {
      throw new StepException(toString() + " : '" +
                                column + "' column is not defined. Please check the wrangling step."
      );
    }
    return masked;
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
      } else if (c == 'x') {
        masked.append(c);
        index++;
      } else {
        masked.append(c);
      }
    }
    return masked.toString();
  }

  private String maskShuffle(String str, int seed) {
    final String cons = "bcdfghjklmnpqrstvwxz";
    final String vowel = "aeiouy";
    final String digit = "0123456789";

    Random r = new Random(seed);
    char data[] = str.toCharArray();

    for (int n = 0; n < data.length; ++ n) {
      char ln = Character.toLowerCase(data[n]);
      if (cons.indexOf(ln) >= 0)
        data[n] = randomChar(r, cons, ln != data[n]);
      else if (vowel.indexOf(ln) >= 0)
        data[n] = randomChar(r, vowel, ln != data[n]);
      else if (digit.indexOf(ln) >= 0)
        data[n] = randomChar(r, digit, ln != data[n]);
    }
    return new String(data);
  }

  private char randomChar(Random r, String cs, boolean uppercase) {
    char c = cs.charAt(r.nextInt(cs.length()));
    return uppercase ? Character.toUpperCase(c) : c;
  }
}

