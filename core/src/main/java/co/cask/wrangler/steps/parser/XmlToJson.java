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

package co.cask.wrangler.steps.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import com.google.gson.JsonObject;
import org.json.JSONException;
import org.json.XML;

import java.util.List;

/**
 * A XML to Json Parser Stage.
 */
@Plugin(type = "udd")
@Name("parse-xml-to-json")
@Usage("parse-xml-to-json <column> [<depth>]")
@Description("Parses a XML document to JSON representation.")
public class XmlToJson extends AbstractDirective {
  // Column within the input row that needs to be parsed as Json
  private String col;
  private int depth;

  public XmlToJson(int lineno, String detail, String col, int depth) {
    super(lineno, detail);
    this.col = col;
    this.depth = depth;
  }

  /**
   * Converts an XML into JSON record.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws DirectiveExecutionException In case CSV parsing generates more record.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(col);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object == null) {
          throw new DirectiveExecutionException(toString() + " : Did not find '" + col + "' in the row.");
        }

        try {
          if (object instanceof String) {
            JsonObject element = JsParser.convert(XML.toJSONObject((String) object)).getAsJsonObject();
            JsParser.flattenJson(element, col, 1, depth, row);
            row.remove(idx);
          } else {
            throw new DirectiveExecutionException(
              String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                            col, object != null ? object.getClass().getName() : "null")
            );
          }
        } catch (JSONException e) {
          throw new DirectiveExecutionException(toString() + " : " + e.getMessage());
        }
      }
    }
    return rows;
  }

}
