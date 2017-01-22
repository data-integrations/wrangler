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
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.Specification;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;

import java.text.ParseException;
import java.util.List;

/**
 * Wrangle Pipeline executes steps in the order they are specified.
 */
public final class DefaultPipeline implements Pipeline<Row, StructuredRecord> {
  private Specification specification;
  private PipelineContext context;

  /**
   * Configures the pipeline based on the specification.
   *
   * @param specification Wrangle specification.
   */
  @Override
  public void configure(Specification specification, PipelineContext context) {
    this.specification = specification;
    this.context = context;
  }

  @Override
  public StructuredRecord execute(Row row, Schema schema) throws PipelineException, SkipRowException {
    // Iterate through steps
    try {
      for (Step step : specification.getSteps()) {
        row = (Row) step.execute(row, context);
      }
    } catch (StepException e) {
      throw new PipelineException(e);
    } catch (ParseException e) {
      throw new PipelineException(e);
    }

    return toStructuredRecord(row, schema);
  }

  /**
   * Converts a {@link Row} to a {@link StructuredRecord}.
   *
   * @param row {@link Row} to be converted
   * @param schema Schema of the {@link StructuredRecord} to be created.
   * @return A {@link StructuredRecord} from row.
   */
  private StructuredRecord toStructuredRecord(Row row, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Object value = row.getValue(name);
      if (value != null) {
        builder.set(name, value);
      }
    }
    return builder.build();
  }
}

