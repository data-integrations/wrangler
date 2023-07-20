/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.wrangler.executor;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.ErrorRecord;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.Executor;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.ReportErrorAndProceed;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.utils.OutputSchemaGenerator;
import io.cdap.wrangler.utils.RecordConvertor;
import io.cdap.wrangler.utils.RecordConvertorException;
import io.cdap.wrangler.utils.TransientStoreKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The class <code>RecipePipelineExecutor</code> compiles the recipe and executes the directives.
 */
public final class RecipePipelineExecutor implements RecipePipeline<Row, StructuredRecord, ErrorRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(RecipePipelineExecutor.class);

  private final ErrorRecordCollector collector = new ErrorRecordCollector();
  private final RecordConvertor convertor = new RecordConvertor();
  private final RecipeParser recipeParser;
  private final ExecutorContext context;
  private List<Directive> directives;

  public RecipePipelineExecutor(RecipeParser recipeParser, @Nullable ExecutorContext context) {
    this.context = context;
    this.recipeParser = recipeParser;
  }

  /**
   * Invokes each directives destroy method to perform any cleanup required by each individual directive.
   */
  @Override
  public void close() {
    if (directives == null) {
      return;
    }
    for (Directive directive : directives) {
      try {
        directive.destroy();
      } catch (Throwable t) {
        LOG.warn(t.getMessage(), t);
      }
    }
  }

  /**
   * Executes the pipeline on the input.
   *
   * @param rows List of Input record of type I.
   * @param schema Schema to which the output should be mapped.
   * @return Parsed output list of record of type O
   */
  @Override
  public List<StructuredRecord> execute(List<Row> rows, Schema schema) throws RecipeException {
    try {
      return convertor.toStructureRecord(execute(rows), schema);
    } catch (RecordConvertorException e) {
      throw new RecipeException("Problem converting into output record. Reason : " + e.getMessage(), e);
    }
  }

  /**
   * Executes the pipeline on the input.
   *
   * @param rows List of input record of type I.
   * @return Parsed output list of record of type I
   */
  @Override
  public List<Row> execute(List<Row> rows) throws RecipeException {
    List<Directive> directives = getDirectives();
    List<String> messages = new ArrayList<>();
    List<Row> results = new ArrayList<>();
    int i = 0;
    int directiveIndex = 0;
    // Initialize schema with input schema from TransientStore if running in service env (design-time) / testing env
    boolean designTime = context != null && context.getEnvironment() != null &&
      (context.getEnvironment().equals(ExecutorContext.Environment.SERVICE) ||
      context.getEnvironment().equals(ExecutorContext.Environment.TESTING));

    try {
      collector.reset();
      while (i < rows.size()) {
        messages.clear();
        // Resets the scope of local variable.
        if (context != null) {
          context.getTransientStore().reset(TransientVariableScope.LOCAL);
        }
        Schema schema = designTime ? context.getTransientStore().get(TransientStoreKeys.INPUT_SCHEMA) : null;

        List<Row> cumulativeRows = rows.subList(i, i + 1);
        directiveIndex = 0;
        try {
          for (Executor<List<Row>, List<Row>> directive : directives) {
            try {
              directiveIndex++;
              cumulativeRows = directive.execute(cumulativeRows, context);
              if (cumulativeRows.size() < 1) {
                break;
              }
              if (designTime && schema != null) {
                Schema directiveOutputSchema = directive.getOutputSchema(schema);
                schema = directiveOutputSchema != null ? directiveOutputSchema
                  : OutputSchemaGenerator.generateOutputSchema(schema, getRowUnion(cumulativeRows));
              }
            } catch (ReportErrorAndProceed e) {
              messages.add(String.format("%s (ecode: %d)", e.getMessage(), e.getCode()));
              collector
                .add(new ErrorRecord(rows.subList(i, i + 1).get(0), String.join(",", messages), e.getCode(), true));
              cumulativeRows = new ArrayList<>();
              break;
            } catch (RecordConvertorException e) {
              throw new RecipeException("Error while generating schema: " + e.getMessage(), e);
            }
          }
          // After executing all directives on a row, take union of previous row's schema and this one's schema
          // to ensure all output columns are included in final output schema
          if (designTime && schema != null) {
            Schema previousRowSchema = context.getTransientStore().get(TransientStoreKeys.OUTPUT_SCHEMA);
            // If this is the first row, initialize previousRowSchema with input schema fields for all columns in output
            if (previousRowSchema == null) {
              Schema inputSchema = context.getTransientStore().get(TransientStoreKeys.INPUT_SCHEMA);
              List<Schema.Field> inputFields = new ArrayList<>();
              for (Pair<String, Object> field : getRowUnion(cumulativeRows).getFields()) {
                Schema.Field existing = inputSchema.getField(field.getFirst());
                if (existing != null) {
                  inputFields.add(existing);
                }
              }
              if (!inputFields.isEmpty()) {
                previousRowSchema = Schema.recordOf(inputFields);
              }
            }
            context.getTransientStore().set(TransientVariableScope.GLOBAL, TransientStoreKeys.OUTPUT_SCHEMA,
                                            OutputSchemaGenerator.getSchemaUnion(previousRowSchema, schema));
          }
          results.addAll(cumulativeRows);
        } catch (ErrorRowException e) {
          messages.add(String.format("%s", e.getMessage()));
          collector
            .add(new ErrorRecord(rows.subList(i, i + 1).get(0), String.join(",", messages), e.getCode(),
              e.isShownInWrangler()));
        }
        ++i;
      }
    } catch (DirectiveExecutionException e) {
      throw new RecipeException(e.getMessage(), e, i, directiveIndex);
    }
    return results;
  }

  /**
   * Returns records that are errored out.
   *
   * @return records that have errored out.
   */
  @Override
  public List<ErrorRecord> errors() {
    return collector.get();
  }

  private List<Directive> getDirectives() throws RecipeException {
    if (directives == null) {
      this.directives = recipeParser.parse();
    }
    return directives;
  }

  public static Row getRowUnion(List<Row> rows) {
    Row union = new Row();
    for (Row row : rows) {
      for (int i = 0; i < row.width(); ++i) {
        if (union.find(row.getColumn(i)) == -1) {
          union.add(row.getColumn(i), row.getValue(i));
        }
      }
    }
    return union;
  }
}
