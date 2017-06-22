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

package co.cask.wrangler.executor;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.pipeline.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.utils.RecordConvertor;
import co.cask.wrangler.utils.RecordConvertorException;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrangle Pipeline executes stepRegistry in the order they are specified.
 */
public final class PipelineExecutor implements Pipeline<Record, StructuredRecord, ErrorRecord> {
  private Directives directives;
  private PipelineContext context;
  private List<Step> steps;
  private final ErrorRecordCollector collector = new ErrorRecordCollector();
  private RecordConvertor convertor = new RecordConvertor();

  /**
   * Configures the pipeline based on the directives.
   *
   * @param directives Wrangle directives.
   */
  @Override
  public void configure(Directives directives, PipelineContext context) throws PipelineException {
    this.directives = directives;
    this.context = context;
    try {
      this.steps = directives.getSteps();
    } catch (DirectiveParseException e) {
      throw new PipelineException(
        String.format(e.getMessage())
      );
    }
  }

  /**
   * Executes the pipeline on the input.
   *
   * @param records List of Input record of type I.
   * @param schema Schema to which the output should be mapped.
   * @return Parsed output list of record of type O
   */
  @Override
  public List<StructuredRecord> execute(List<Record> records, Schema schema)
    throws PipelineException {
    records = execute(records);
    try {
      List<StructuredRecord> output = convertor.toStructureRecord(records, schema);
      return output;
    } catch (RecordConvertorException e) {
      throw new PipelineException("Problem converting into output record. Reason : " + e.getMessage());
    }
  }

  /**
   * Executes the pipeline on the input.
   *
   * @param records List of input record of type I.
   * @return Parsed output list of record of type I
   */
  @Override
  public List<Record> execute(List<Record> records) throws PipelineException {
    List<Record> results = Lists.newArrayList();
    try {
      int i = 0;
      collector.reset();
      while (i < records.size()) {
        List<Record> newRecords = records.subList(i, i+1);
        try {
          for (Step step : steps) {
            newRecords = step.execute(newRecords, context);
            if (newRecords.size() < 1) {
              break;
            }
          }
          if(newRecords.size() > 0) {
            results.addAll(newRecords);
          }
        } catch (ErrorRecordException e) {
          collector.add(new ErrorRecord(newRecords.get(0), e.getMessage(), e.getCode()));
        }
        i++;
      }
    } catch (StepException  e) {
      throw new PipelineException(e);
    }
    return results;
  }

  /**
   * Returns records that are errored out.
   *
   * @return records that have errored out.
   */
  @Override
  public List<ErrorRecord> errors() throws PipelineException {
    return collector.get();
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
