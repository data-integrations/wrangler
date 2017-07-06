/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.directives.xml;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.ximpleware.AutoPilot;
import com.ximpleware.NavException;
import com.ximpleware.VTDNav;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import java.util.List;

/**
 * A Executor to extract a single XML element using XPath.
 *
 * <p>
 *   TODO: This code has to be moved out into a plugin due to VTDNav once we have
 *   the plugin framework.
 * </p>
 *
 */
@Plugin(type = Directive.Type)
@Name(XPathElement.NAME)
@Description("Extract a single XML element or attribute using XPath.")
public class XPathElement implements Directive {
  public static final String NAME = "xpath";
  private String column;
  private String destination;
  private String xpath;
  private String attribute;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("xpath", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("source")).value();
    this.destination = ((ColumnName) args.value("destination")).value();
    this.xpath = ((Text) args.value("xpath")).value();
    this.attribute = XPathElement.extractAttributeFromXPath(xpath.trim());
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof VTDNav) {
          VTDNav vNav = (VTDNav) row.getValue(idx);
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
                  row.addOrSet(destination, title);
                  found = true;
                }
              }
            } else {
              if (ap.evalXPath() != -1) {
                int val = vNav.getAttrVal(attribute);
                if (val != -1) {
                  row.addOrSet(destination, vNav.toString(val));
                  found = true;
                }
              }
            }
            if(!found) {
              row.addOrSet(destination, null);
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
        row.addOrSet(destination, null);
      }
    }
    return rows;
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
