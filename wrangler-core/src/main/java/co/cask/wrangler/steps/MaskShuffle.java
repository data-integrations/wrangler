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
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.ArrayList;
import java.util.List;
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
 *        Step step = new MaskNumber(lineno, line, "ssn", "XXX-XX-####", 1);
 *        Step step = new MaskNumber(lineno, line, "amex", "XXXX-XXXXXX-X####", 1);
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
 *       Step step = new MaskNumber(lineno, line, "150 Mars Avenue, Marcity, Mares", 2);
 *     </pre>
 *   </blockquote>
 * </p>
 */
@Usage(directive = "mask-shuffle", usage = "mask-shuffle <column>")
public class MaskShuffle extends AbstractStep {
  // Column on which to apply mask.
  private final String column;

  public MaskShuffle(int lineno, String detail, String column) {
    super(lineno, detail);
    this.column = column;
  }

  /**
   * Masks the column specified using either substitution method or shuffling.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Record} with masked column.
   * @throws StepException thrown when there is issue with masking
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      Record masked = new Record(record);
      int idx = record.find(column);
      if (idx != -1) {
        masked.setValue(idx, maskShuffle((String) record.getValue(idx), 0));
      } else {
        throw new StepException(toString() + " : '" +
                                  column + "' column is not defined. Please check the wrangling step."
        );
      }
      results.add(masked);
    }

    return results;
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

