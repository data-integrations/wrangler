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
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.ximpleware.AutoPilot;
import com.ximpleware.NavException;
import com.ximpleware.VTDNav;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import java.util.List;

/**
 * A Step to extract a single XML element using XPath.
 *
 * <p>
 *   TODO: This code has to be moved out into a plugin due to VTDNav once we have
 *   the plugin framework.
 * </p>
 *
 */
@Plugin(type = "udd")
@Name("xpath")
@Usage("xpath <column> <destination> <xpath>")
@Description("Extract a single XML element or attribute using XPath.")
public class XPathElement extends AbstractStep {
  private final String column;
  private final String destination;
  private final String xpath;
  private final String attribute;

  public XPathElement(int lineno, String directive, String column, String destination, String xpath) {
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
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object instanceof VTDNav) {
          VTDNav vNav = (VTDNav) record.getValue(idx);
          AutoPilot ap = new AutoPilot(vNav);
          try {
            int tokenCount = vNav.getTokenCount();
            String token = null;
            String nsPrefix = null;
            String nsUrl = null;
            for ( int i = 0; i < tokenCount; i++ ) {
              token = vNav.toNormalizedString( i );
              if ( vNav.startsWith( i, "xmlns:" ) ) {
                nsPrefix = token.substring( token.indexOf( ":" ) + 1 );
                nsUrl = vNav.toNormalizedString( i + 1 );
                ap.declareXPathNameSpace( nsPrefix, nsUrl );
              }// if
            }// for
            boolean found = false;
            ap.selectXPath(xpath);
            if (attribute == null) {
              if (ap.evalXPath() != -1) {
                int val = vNav.getText();
                if (val != -1) {
                  String title = vNav.getXPathStringVal();
                  record.addOrSet(destination, title);
                  found = true;
                }
              }
            } else {
              if (ap.evalXPath() != -1) {
                int val = vNav.getAttrVal(attribute);
                if (val != -1) {
                  record.addOrSet(destination, vNav.toString(val));
                  found = true;
                }
              }
            }
            if(!found) {
              record.addOrSet(destination, null);
            }
          } catch (XPathParseException | XPathEvalException | NavException e) {
            throw new StepException(
              String.format("%s : Failed in extracting information using xpath element '%s' for field '%s'. %s",
                            toString(), xpath, column, e.getMessage())
            );
          }
        } else {
          throw new StepException(
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

  public static String extractAttributeFromXPath(String path) {
    int index = path.lastIndexOf('/');
    if (index != -1) {
      String attribute = path.substring(index + 1);
      int squareIndex = attribute.indexOf('[');
      if (squareIndex == -1) {
        int attrIndex = attribute.indexOf('@');
        if (attrIndex != -1) {
          String attr = attribute.substring(attrIndex + 1);
          return attr;
        }
      }
    }
    return null;
  }
}
