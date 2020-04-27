/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.directives.parser;

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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * A step for parsing the HL7 Message.
 */
@Plugin(type = Directive.TYPE)
@Name("parse-as-hl7")
@Categories(categories = { "parser", "hl7"})
@Description("Parses <column> for Health Level 7 Version 2 (HL7 V2) messages; <depth> indicates at which point " +
  "JSON object enumeration terminates.")
public class HL7Parser implements Directive, Lineage {
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
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Parsed column '%s' as HL7 record", column)
      .all(Many.columns(column), Many.columns(column))
      .build();
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      try {
        int idx = row.find(column);
        if (idx != -1) {
          Object object = row.getValue(idx);

          if (object == null) {
            throw new DirectiveExecutionException(
              NAME, String.format("Column '%s' has null value. It should be a non-null 'String'.", column)
            );
          }

          // Handling the first parsing on HL7 message
          if (object instanceof String) {
            Message message = parser.parse((String) object);
            HL7MessageVisitor visitor = new HL7MessageVisitor(row, column + "_hl7", depth);
            MessageVisitors.visit(message,
                                  MessageVisitors.visitPopulatedElements(visitor)).getDelegate();
          } else {
            throw new DirectiveExecutionException(
              NAME, String.format("Column '%s' has invalid type '%s'. It should be of type 'String'.",
                                  column, object.getClass().getSimpleName())
            );
          }

        }
      } catch (HL7Exception e) {
        throw new DirectiveExecutionException(NAME, e.getMessage(), e);
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

    HL7MessageVisitor(Row row, String column, int depth) {
      this.row = row;
      this.column = column;
      this.depth = depth;
    }

    @Override
    public boolean start(Message message) {
      return true;
    }

    @Override
    public boolean end(Message message) {
      JsParser.jsonFlatten(segments, column, 1, depth, row);
      return true;
    }

    @Override
    public boolean start(Group group, Location location) {
      return true;
    }

    @Override
    public boolean end(Group group, Location location) {
      return true;
    }

    @Override
    public boolean start(Segment segment, Location location) {
      segmentObject = new JsonObject();
      return true;
    }

    @Override
    public boolean end(Segment segment, Location location) {
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
    public boolean start(Field field, Location location) {
      return true;
    }

    @Override
    public boolean end(Field field, Location location) {
      return true;
    }

    @Override
    public boolean start(Composite composite, Location location) {
      inComposite = true;
      compositeObject = new JsonObject();
      return true;
    }

    @Override
    public boolean end(Composite composite, Location location) {
      segmentObject.add(Integer.toString(location.getField()), compositeObject);
      inComposite = false;
      return true;
    }

    @Override
    public boolean visit(Primitive primitive, Location location) {
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
