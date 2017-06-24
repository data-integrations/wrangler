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
import org.unix4j.Unix4j;

import java.util.List;

/**
 * A step that implements unix cut directive.
 */
@Plugin(type = "udd")
@Name("cut-character")
@Usage("cut-character <source> <destination> <type> <range|indexes>")
@Description("UNIX-like 'cut' directive for splitting text.")
public class CharacterCut extends AbstractDirective {
  private String source;
  private String destination;
  private String range;

  public CharacterCut(int lineno, String detail, String source, String destination, String range) {
    super(lineno, detail);
    this.source = source;
    this.destination = destination;
    this.range = range;
  }

  /**
   * Character-based 'cut' operations.
   *
   * @param records Input {@link Record} to be wrangled by this step
   * @param context Specifies the context of the pipeline
   * @return Transformed {@link Record}
   * @throws DirectiveExecutionException thrown when type of 'col' is not STRING
   */
  @Override
  public List<Record> execute(List<Record> records, RecipeContext context) throws DirectiveExecutionException {
    for (Record record : records) {
      int idx = record.find(source);
      if (idx != -1) {
        Object value = record.getValue(idx);
        if (value instanceof String) {
          String result = Unix4j.fromString((String) value).cut("-c", range).toStringResult();
          record.addOrSet(destination, result);
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          value.getClass().getName(), source)
          );
        }
      } else {
        throw new DirectiveExecutionException(toString() + " : Source column '" + source + "' does not exist in the record.");
      }
    }
    return records;
  }
}
