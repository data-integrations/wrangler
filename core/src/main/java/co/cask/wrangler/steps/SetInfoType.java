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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.*;
import co.cask.wrangler.api.i18n.Messages;
import co.cask.wrangler.api.i18n.MessagesFactory;

import java.util.List;

/**
 * A directive that defines a information type of the column who's life-expectancy is only within the record.
 *
 * The type set as transient variable is available to all the directives after that. But, it's
 * not available beyond the input record.
 */
@Usage(
  directive = "set-info-type",
  usage = "set-info-type <column> <type>",
  description = "Manually indicate the information type of a column."
)
public class SetInfoType extends AbstractStep {
  private static final Messages MSG = MessagesFactory.getMessages();

  //column to set type for
  private String col;

  //set column to be this type
  private String type;

  public SetInfoType(int lineno, String detail, String col, String type) {
    super(lineno, detail);
    this.col = col;
    this.type = type;
  }


  /**
   * Set data type of the column and store the type information in transient state
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return same records as input
   * @throws StepException Thrown when the column to set type for doesn't exist.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException{
    for (Record record : records) {
      int idx = record.find(col);
      if (idx == -1) {
        throw new StepException(
          String.format(
            "%s : %s column doesn't exist", toString(), col
          )
        );
      }
    }

    //set type in transient store
    TransientStore store = context.getTransientStore();
    String transientVarName = col + "_data_type";
    store.set(transientVarName, type);
    return records;
  }
}
