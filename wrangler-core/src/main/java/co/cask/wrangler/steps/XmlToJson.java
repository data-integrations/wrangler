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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import org.json.JSONException;
import org.json.XML;

import java.util.ArrayList;
import java.util.List;

/**
 * A XML to Json Parser Stage.
 */
@Usage(directive = "parse-as-xml", usage = "parse-as-xml <column>")
public class XmlToJson extends AbstractStep {
  // Column within the input row that needs to be parsed as Json
  private String col;

  public XmlToJson(int lineno, String detail, String col) {
    super(lineno, detail);
    this.col = col;
  }

  /**
   * Converts an XML into JSON record.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws StepException In case CSV parsing generates more record.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();

    for (Record record : records) {
      int idx = record.find(col);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object == null) {
          throw new StepException(toString() + " : Did not find '" + col + "' in the record.");
        }

        try {
          if (object instanceof String) {
            record.setValue(idx, XML.toJSONObject((String) object));
          } else {
            throw new StepException(
              String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                            col, object != null ? object.getClass().getName() : "null")
            );
          }
        } catch (JSONException e) {
          throw new StepException(toString() + " : " + e.getMessage());
        }
        results.add(record);
      }
    }
    return results;
  }

}
