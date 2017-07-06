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

package co.cask.wrangler.executor;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Executor;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.RecipeException;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.utils.RecordConvertor;
import co.cask.wrangler.utils.RecordConvertorException;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Wrangle RecipePipeline executes stepRegistry in the order they are specified.
 */
@Beta
public final class ParallelRecipePipelineExecutor implements RecipePipeline<Row, StructuredRecord, ErrorRecord> {
  private RecipeParser directives;
  private ExecutorContext context;
  private RecordConvertor convertor = new RecordConvertor();
  private int threads = Runtime.getRuntime().availableProcessors();
  private ExecutorService executor = Executors.newFixedThreadPool(threads,
                                                                  new ThreadFactoryBuilder().setDaemon(true).build());

  /**
   * Configures the pipeline based on the directives.
   *
   * @param directives Wrangle directives.
   */
  @Override
  public void configure(RecipeParser directives, ExecutorContext context) {
    this.directives = directives;
    this.context = context;
  }

  /**
   * Executes the pipeline on the input.
   *
   * @param rows List of Input record of type I.
   * @param schema Schema to which the output should be mapped.
   * @return Parsed output list of record of type O
   */
  @Override
  public List<StructuredRecord> execute(List<Row> rows, Schema schema)
    throws RecipeException {
    rows = execute(rows);
    try {
      List<StructuredRecord> output = convertor.toStructureRecord(rows, schema);
      return output;
    } catch (RecordConvertorException e) {
      throw new RecipeException("Problem converting into output record. Reason : " + e.getMessage());
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
    try {
      int chunkSize = rows.size() / threads;
      if (chunkSize == 0) {
        chunkSize = rows.size();
      }

      int startIndex = 0;
      Map<Integer, Future<List<Row>>> futures = new TreeMap<>();

      final List<Executor> directives = this.directives.parse();
      while (startIndex < rows.size()) {
        int endIndex = startIndex + chunkSize;
        if (endIndex > rows.size()) {
          endIndex = rows.size();
        }

        final List<Row> newRows = rows.subList(startIndex, endIndex);

        Future<List<Row>> future = executor.submit(new Callable<List<Row>>() {
          private List<Row> records = newRows;
          @Override
          public List<Row> call() throws Exception {
            for (Executor step : directives) {
              // If there are no rows, then we short-circuit the processing and break out.
              if (records.size() < 1) {
                break;
              }
              records = step.execute(records, context);
            }
            return records;
          }
        });

        futures.put(startIndex, future);
        startIndex = endIndex;
      }

      List<List<Row>> result = new ArrayList<>(threads);
      for (Future<List<Row>> future : futures.values()) {
        result.add(future.get());
      }
      return Lists.newArrayList(Iterables.concat(result));
    } catch (Exception e) {
      throw new RecipeException(e);
    }
  }

  /**
   * Returns records that are errored out.
   *
   * @return records that have errored out.
   */
  @Override
  public List<ErrorRecord> errors() throws RecipeException {
    return new ArrayList<>();
  }

  /**
   * Converts a {@link Row} to a {@link StructuredRecord}.
   *
   * @param rows {@link Row} to be converted
   * @param schema Schema of the {@link StructuredRecord} to be created.
   * @return A {@link StructuredRecord} from record.
   */
  private List<StructuredRecord> toStructuredRecord(List<Row> rows, Schema schema) {
    List<StructuredRecord> results = new ArrayList<>();
    for (Row row : rows) {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      List<Schema.Field> fields = schema.getFields();
      for (Schema.Field field : fields) {
        String name = field.getName();
        Object value = row.getValue(name);
        if (value != null) {
          builder.set(name, value);
        }
      }
      results.add(builder.build());
    }
    return results;
  }
}
