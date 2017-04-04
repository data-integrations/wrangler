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
import com.ximpleware.ParseException;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A XML Parser.
 */
@Usage(
  directive = "parse-as-xml",
  usage = "parse-as-xml <column>",
  description = "Parses a column as XML."
)
public class XmlParser extends AbstractStep {
  // Column within the input row that needs to be parsed as CSV
  private String col;
  private final VTDGen vg = new VTDGen();


  public XmlParser(int lineno, String detail, String col) {
    super(lineno, detail);
    this.col = col;
  }

  /**
   * Parses a give column in a {@link Record} as a XML.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Record containing multiple columns based on CSV parsing.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context)
    throws StepException {

    for (Record record : records) {
      int idx = record.find(col);
      if (idx == -1) {
        continue; // didn't find the column.
      }

      Object object = record.getValue(idx);
      if (object == null) {
        continue; // If it's null keep it as null.
      }

      if (object instanceof String) {
        String xml = (String) object;
        vg.setDoc(xml.getBytes(StandardCharsets.UTF_8));
        try {
          vg.parse(true);
        } catch (ParseException e) {
          e.printStackTrace();
        }
        VTDNav vn = vg.getNav();
        record.setValue(idx, vn);
      }
    }
    return records;
  }
}