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
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import com.ximpleware.AutoPilot;
import com.ximpleware.NavException;
import com.ximpleware.VTDNav;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * A Directive to extract XML element as an JSON array using XPath.
 *
 * <p>
 *   TODO: This code has to be moved out into a plugin due to VTDNav once we have
 *   the plugin framework.
 * </p>
 *
 */
@Plugin(type = "udd")
@Name("xpath-array")
@Usage("xpath-array <column> <destination> <xpath>")
@Description("Extract XML element or attributes as JSON array using XPath.")
public class XPathArrayElement extends AbstractDirective {
  private final String column;
  private final String destination;
  private final String xpath;
  private final String attribute;

  public XPathArrayElement(int lineno, String directive, String column, String destination, String xpath) {
    super(lineno, directive);
    this.column = column;
    this.destination = destination;
    this.xpath = xpath;
    this.attribute = XPathElement.extractAttributeFromXPath(xpath.trim());
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, RecipeContext context) throws DirectiveExecutionException {
    List<String> values = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object instanceof VTDNav) {
          VTDNav vn = (VTDNav) record.getValue(idx);
          AutoPilot ap = new AutoPilot(vn);
          try {
            int tokenCount = vn.getTokenCount();
            String token = null;
            String nsPrefix = null;
            String nsUrl = null;
            for ( int i = 0; i < tokenCount; i++ ) {
              token = vn.toNormalizedString( i );
              if ( vn.startsWith( i, "xmlns:" ) ) {
                nsPrefix = token.substring( token.indexOf( ":" ) + 1 );
                nsUrl = vn.toNormalizedString( i + 1 );
                ap.declareXPathNameSpace( nsPrefix, nsUrl );
              }// if
            }// for
            ap.selectXPath(xpath);
            if (attribute == null) {
              int i = 0, j = 0;
              while (( i = ap.evalXPath()) != -1) {
                int val = vn.getText();
                if (val != -1) {
                  values.add(vn.getXPathStringVal());
                }
              }
              JsonArray array = new JsonArray();
              for (String value : values) {
                array.add(new JsonPrimitive(value));
              }
              record.addOrSet(destination, array);
            } else {
              int i = 0, j = 0;
              while (( i = ap.evalXPath()) != -1) {
                int val = vn.getAttrVal(attribute);
                if (val != -1) {
                  values.add(vn.toString(val));
                }
              }
              JsonArray array = new JsonArray();
              for (String value : values) {
                array.add(new JsonPrimitive(value));
              }
              record.addOrSet(destination, array);
            }
          } catch (XPathParseException | XPathEvalException | NavException e) {
            throw new DirectiveExecutionException(
              String.format("%s : Failed in extracting information using xpath element '%s' for field '%s'. %s",
                            toString(), xpath, column, e.getMessage())
            );
          }
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type parsed XML. 'parse-as-xml' first",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
      } else {
        record.addOrSet(destination, null);
      }
    }
    return records;
  }
}
