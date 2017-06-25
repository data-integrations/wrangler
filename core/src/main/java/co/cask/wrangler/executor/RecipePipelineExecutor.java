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
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.RecipeException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.utils.RecordConvertor;
import co.cask.wrangler.utils.RecordConvertorException;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrangle RecipePipeline executes stepRegistry in the order they are specified.
 */
public final class RecipePipelineExecutor implements RecipePipeline<Record, StructuredRecord, ErrorRecord> {
  private RecipeContext context;
  private List<Directive> directives;
  private final ErrorRecordCollector collector = new ErrorRecordCollector();
  private RecordConvertor convertor = new RecordConvertor();

  /**
   * Configures the pipeline based on the directives.
   *
   * @param parser Wrangle directives parser.
   */
  @Override
  public void configure(RecipeParser parser, RecipeContext context) throws RecipeException {
    this.context = context;
    try {
      this.directives = parser.parse();
    } catch (DirectiveParseException e) {
      throw new RecipeException(
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
    throws RecipeException {
    records = execute(records);
    try {
      List<StructuredRecord> output = convertor.toStructureRecord(records, schema);
      return output;
    } catch (RecordConvertorException e) {
      throw new RecipeException("Problem converting into output record. Reason : " + e.getMessage());
    }
  }

  /**
   * Executes the pipeline on the input.
   *
   * @param records List of input record of type I.
   * @return Parsed output list of record of type I
   */
  @Override
  public List<Record> execute(List<Record> records) throws RecipeException {
    List<Record> results = Lists.newArrayList();
    try {
      int i = 0;
      collector.reset();
      while (i < records.size()) {
        List<Record> newRecords = records.subList(i, i+1);
        try {
          for (Directive directive : directives) {
            newRecords = directive.execute(newRecords, context);
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
    } catch (DirectiveExecutionException e) {
      throw new RecipeException(e);
    }
    return results;
  }

  /**
   * Returns records that are errored out.
   *
   * @return records that have errored out.
   */
  @Override
  public List<ErrorRecord> errors() throws RecipeException {
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
