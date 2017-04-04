/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package co.cask.wrangler.internal;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
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
 * Wrangle Pipeline executes stepRegistry in the order they are specified.
 */
@Beta
public final class ParallelPipelineExecutor implements Pipeline<Record, StructuredRecord> {
  private Directives directives;
  private PipelineContext context;
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
  public void configure(Directives directives, PipelineContext context) {
    this.directives = directives;
    this.context = context;
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
    try {
      int chunkSize = records.size() / threads;
      if (chunkSize == 0) {
        chunkSize = records.size();
      }

      int startIndex = 0;
      Map<Integer, Future<List<Record>>> futures = new TreeMap<>();

      final List<Step> steps = directives.getSteps();
      while (startIndex < records.size()) {
        int endIndex = startIndex + chunkSize;
        if (endIndex > records.size()) {
          endIndex = records.size();
        }

        final List<Record> newRecords = records.subList(startIndex, endIndex);

        Future<List<Record>> future = executor.submit(new Callable<List<Record>>() {
          private List<Record> records = newRecords;
          @Override
          public List<Record> call() throws Exception {
            for (Step step : steps) {
              // If there are no records, then we short-circuit the processing and break out.
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

      List<List<Record>> result = new ArrayList<>(threads);
      for (Future<List<Record>> future : futures.values()) {
        result.add(future.get());
      }
      return Lists.newArrayList(Iterables.concat(result));
    } catch (Exception e) {
      throw new PipelineException(e);
    }
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
