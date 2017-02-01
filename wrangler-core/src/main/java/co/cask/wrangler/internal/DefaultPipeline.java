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

package co.cask.wrangler.internal;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrangle Pipeline executes steps in the order they are specified.
 */
public final class DefaultPipeline implements Pipeline<Record, StructuredRecord> {
  private Directives directives;
  private PipelineContext context;

  /**
   * Configures the pipeline based on the directives.
   *
   * @param directives Wrangle directives.
   */
  @Override
  public void configure(Directives directives, PipelineContext context) {
    this.directives = directives;
    this.context = context;
  }

  @Override
  public List<StructuredRecord> execute(List<Record> records, Schema schema) throws PipelineException {
    // Iterate through steps
    try {
      for (Step step : directives.getSteps()) {
        records = step.execute(records, context);
      }
    } catch (StepException e) {
      throw new PipelineException(e);
    } catch (DirectiveParseException e) {
      throw new PipelineException(e);
    }

    return toStructuredRecord(records, schema);
  }

  /**
   * Converts a {@link Record} to a {@link StructuredRecord}.
   *
   * @param records {@link Record} to be converted
   * @param schema Schema of the {@link StructuredRecord} to be created.
   * @return A {@link StructuredRecord} from record.
   */
  private List<StructuredRecord> toStructuredRecord(List<Record> records, Schema schema) {
    List<StructuredRecord> results = new ArrayList<>();
    for (Record record : records) {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      List<Schema.Field> fields = schema.getFields();
      for (Schema.Field field : fields) {
        String name = field.getName();
        Object value = record.getValue(name);
        if (value != null) {
          builder.set(name, value);
        }
      }
      results.add(builder.build());
    }
    return results;
  }
}

