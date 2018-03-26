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

package co.cask.directives.parser;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.Location;
import ca.uhn.hl7v2.model.Composite;
import ca.uhn.hl7v2.model.Field;
import ca.uhn.hl7v2.model.Group;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.MessageVisitor;
import ca.uhn.hl7v2.model.MessageVisitors;
import ca.uhn.hl7v2.model.Primitive;
import ca.uhn.hl7v2.model.Segment;
import ca.uhn.hl7v2.parser.GenericModelClassFactory;
import ca.uhn.hl7v2.parser.ModelClassFactory;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.validation.impl.NoValidation;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Numeric;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.List;

/**
 * A step for parsing the HL7 Message.
 */
@Plugin(type = Directive.Type)
@Name("parse-as-hl7")
@Categories(categories = { "parser", "hl7"})
@Description("Parses <column> for Health Level 7 Version 2 (HL7 V2) messages; <depth> indicates at which point " +
  "JSON object enumeration terminates.")
public class HL7Parser implements Directive {
  public static final String NAME = "parse-as-hl7";
  private String column;
  private HapiContext context;
  private Parser parser;
  private int depth;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("depth", TokenType.NUMERIC, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    if (args.contains("depth")) {
      this.depth = ((Numeric) args.value("depth")).value().intValue();
    } else {
      this.depth = Integer.MAX_VALUE;
    }
    context = new DefaultHapiContext();
    context.setValidationContext(new NoValidation());
    ModelClassFactory modelClassFactory = new GenericModelClassFactory();
    parser = new PipeParser(modelClassFactory);
    parser.getParserConfiguration().setAllowUnknownVersions(true);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      try {
        int idx = row.find(column);
        if (idx != -1) {
          Object object = row.getValue(idx);
          // Handling the first parsing on HL7 message
          if (object instanceof String) {
            Message message = parser.parse((String) object);
            HL7MessageVisitor visitor = new HL7MessageVisitor(row, column + "_hl7", depth);
            MessageVisitors.visit(message,
                                  MessageVisitors.visitPopulatedElements(visitor)).getDelegate();
          } else {
            throw new DirectiveExecutionException(
              String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.",
                            toString(), object != null ? object.getClass().getName() : "null", column)
            );
          }
        }
      } catch (HL7Exception e) {
        throw new DirectiveExecutionException(toString() + " : " + e.getMessage());
      }
    }
    return rows;
  }

  /**
   * HL7 Message Visitor.
   */
  private final class HL7MessageVisitor implements MessageVisitor {
    private final Row row;
    private final String column;
    private final int depth;
    private JsonObject segments = new JsonObject();
    private JsonObject segmentObject = new JsonObject();
    private JsonObject compositeObject = new JsonObject();
    private boolean inComposite = false;

    public HL7MessageVisitor(Row row, String column, int depth) {
      this.row = row;
      this.column = column;
      this.depth = depth;
    }

    @Override
    public boolean start(Message message) throws HL7Exception {
      return true;
    }

    @Override
    public boolean end(Message message) throws HL7Exception {
      JsParser.jsonFlatten(segments, column, 1, depth, row);
      return true;
    }

    @Override
    public boolean start(Group group, Location location) throws HL7Exception {

      return true;
    }

    @Override
    public boolean end(Group group, Location location) throws HL7Exception {
      return true;
    }

    @Override
    public boolean start(Segment segment, Location location) throws HL7Exception {
      segmentObject = new JsonObject();
      return true;
    }

    @Override
    public boolean end(Segment segment, Location location) throws HL7Exception {
      if (!segments.has(segment.getName())) {
        segments.add(segment.getName(), segmentObject);
      } else {
        Object object = segments.get(segment.getName());
        if (!(object instanceof JsonArray)) {
          JsonObject o = (JsonObject) segments.get(segment.getName());
          JsonArray a = new JsonArray();
          a.add(o);
          a.add(segmentObject);
          segments.add(segment.getName(), a);
        }
      }
      return true;
    }

    @Override
    public boolean start(Field field, Location location) throws HL7Exception {
      return true;
    }

    @Override
    public boolean end(Field field, Location location) throws HL7Exception {
      return true;
    }

    @Override
    public boolean start(Composite composite, Location location) throws HL7Exception {
      inComposite = true;
      compositeObject = new JsonObject();
      return true;
    }

    @Override
    public boolean end(Composite composite, Location location) throws HL7Exception {
      segmentObject.add(Integer.toString(location.getField()), compositeObject);
      inComposite = false;
      return true;
    }

    @Override
    public boolean visit(Primitive primitive, Location location) throws HL7Exception {
      if (inComposite) {
        compositeObject.addProperty(Integer.toString(location.getComponent()), primitive.getValue());
      } else {
        String fieldComponent = String.format("%d_%d", location.getField(), location.getComponent());
        if (location.getComponent() < 0) {
          fieldComponent = String.format("%d", location.getField());
        }
        segmentObject.addProperty(fieldComponent, primitive.getValue());
      }
      return true;
    }
  }
}
