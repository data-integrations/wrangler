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
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.validation.impl.NoValidation;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

/**
 * A step for parsing the HL7 Message.
 */
public class HL7Parser extends AbstractStep {
  private final String column;
  private final HapiContext context;
  private final Parser parser;

  public HL7Parser(int lineno, String detail, String column) {
    super(lineno, detail);
    this.column = column;
    context = new DefaultHapiContext();
    context.setValidationContext(new NoValidation());
    parser = context.getGenericParser();
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      try {
        int idx = record.find(column);
        if (idx != -1) {
          Object object = record.getValue(idx);
          // Handling the first parsing on HL7 message
          if (object instanceof String) {
            Message message = parser.parse((String) object);
            HL7MessageVisitor visitor = new HL7MessageVisitor(record, column + "_hl7");
            MessageVisitors.visit(message,
                                  MessageVisitors.visitPopulatedElements(visitor)).getDelegate();
          } else {
            throw new StepException(
              String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.",
                            toString(), object != null ? object.getClass().getName() : "null", column)
            );
          }
        }
      } catch (HL7Exception e) {
        throw new StepException(toString() + " : " + e.getMessage());
      }
    }
    return records;
  }

  /**
   * HL7 Message Visitor.
   */
  private final class HL7MessageVisitor implements MessageVisitor {
    private final Record record;
    private JSONObject segments = new JSONObject();
    private JSONObject segmentObject = new JSONObject();
    private JSONObject compositeObject = new JSONObject();
    private boolean inComposite = false;
    private String column;

    public HL7MessageVisitor(Record record, String column) {
      this.record = record;
      this.column = column;
    }

    @Override
    public boolean start(Message message) throws HL7Exception {
      return true;
    }

    @Override
    public boolean end(Message message) throws HL7Exception {
      record.add(column, segments);
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
      segmentObject = new JSONObject();
      return true;
    }

    @Override
    public boolean end(Segment segment, Location location) throws HL7Exception {
      if (!segments.has(segment.getName())) {
        segments.put(segment.getName(), segmentObject);
      } else {
        Object object = segments.get(segment.getName());
        if (!(object instanceof JSONArray)) {
          JSONObject o = (JSONObject) segments.get(segment.getName());
          JSONArray a = new JSONArray();
          a.put(o);
          a.put(segmentObject);
          segments.put(segment.getName(), a);
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
      compositeObject = new JSONObject();
      return true;
    }

    @Override
    public boolean end(Composite composite, Location location) throws HL7Exception {
      segmentObject.put(Integer.toString(location.getField()), compositeObject);
      inComposite = false;
      return true;
    }

    @Override
    public boolean visit(Primitive primitive, Location location) throws HL7Exception {
      if (inComposite) {
        compositeObject.put(Integer.toString(location.getComponent()), primitive.getValue());
      } else {
        String fieldComponent = String.format("%d_%d", location.getField(), location.getComponent());
        if (location.getComponent() < 0) {
          fieldComponent = String.format("%d", location.getField());
        }
        segmentObject.put(fieldComponent, primitive.getValue());
      }
      return true;
    }
  }
}
