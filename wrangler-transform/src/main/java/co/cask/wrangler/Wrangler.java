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

package co.cask.wrangler;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.internal.DefaultPipeline;
import co.cask.wrangler.internal.TextDirectives;
import com.google.common.base.Strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Wrangler - A interactive tool for data data cleansing and transformation.
 *
 * This plugin is an implementation of the transformation that are performed in the
 * backend for operationalizing all the interactive wrangling that is being performed
 * by the user.
 */
@Plugin(type = "transform")
@Name("Wrangler")
@Description("Wrangler - A interactive tool for data cleansing and transformation.")
public class Wrangler extends Transform<StructuredRecord, StructuredRecord> {
  // Plugin configuration.
  private final Config config;

  // Wrangle Execution Pipeline
  private Pipeline pipeline;

  // Output Schema associated with transform output.
  private Schema oSchema;

  // Error counter.
  private long errorCounter;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Wrangler(Config config) {
    this.config = config;
  }

  private class WranglerPipelineContext implements PipelineContext {
    private StageMetrics metrics;
    private String name;

    public WranglerPipelineContext(StageMetrics metrics, String name) {
      this.metrics = metrics;
      this.name = name;
    }

    /**
     * @return Metrics context.
     */
    @Override
    public StageMetrics getMetrics() {
      return metrics;
    }

    /**
     * @return Context name.
     */
    @Override
    public String getContextName() {
      return name;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);

    Schema iSchema = configurer.getStageConfigurer().getInputSchema();

    if (!config.field.equalsIgnoreCase("*")) {
      validateInputSchema(iSchema);
    }

    // Validate the DSL by parsing DSL.
    Directives directives = new TextDirectives(config.specification);
    try {
      directives.getSteps();
    } catch (DirectiveParseException e) {
      throw new IllegalArgumentException(e);
    }

    // Based on the configuration create output schema.
    try {
      oSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of output schema specified is invalid. Please check the format.");
    }

    // Set the output schema.
    configurer.getStageConfigurer().setOutputSchema(oSchema);
  }

  void validateInputSchema(Schema inputSchema) {
    if (inputSchema != null) {
      // Check the existence of field in input schema
      Schema.Field inputSchemaField = inputSchema.getField(config.field);
      if (inputSchemaField == null) {
        throw new IllegalArgumentException(
          "Field " + config.field + " is not present in the input schema");
      }

      // Check that the field type is String or Nullable String
      Schema fieldSchema = inputSchemaField.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (!fieldType.equals(Schema.Type.STRING)) {
        throw new IllegalArgumentException(
          "Type for field  " + config.field + " must be String");
      }
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    // Parse DSL and initialize the wrangle pipeline.
    Directives directives = new TextDirectives(config.specification);
    pipeline = new DefaultPipeline();
    pipeline.configure(directives, new WranglerPipelineContext(context.getMetrics(), context.getStageName()));

    // Based on the configuration create output schema.
    try {
      oSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of output schema specified is invalid. Please check the format.");
    }

    // Initialize the error counter.
    errorCounter = 0;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // Creates a row as starting point for input to the pipeline.
    Record row;
    if (config.field.equalsIgnoreCase("*")) {
      row = new Record();
      for (Schema.Field field : input.getSchema().getFields()) {
        row.add(field.getName(), input.get(field.getName()));
      }
    } else {
      row = new Record(Directives.STARTING_COLUMN, input.get(config.field));
    }

    // Run through the wrangle pipeline, if there is a SkipRecord exception, don't proceed further
    // but just return without emitting any record out.
    List<StructuredRecord> records = new ArrayList<>();
    long start = System.nanoTime();
    try {
      records = pipeline.execute(Arrays.asList(row), oSchema);
    } catch (PipelineException e) {
      getContext().getMetrics().count("pipeline.records.failures", 1);
      errorCounter++;
      return;
    } finally {
      getContext().getMetrics().gauge("pipeline.record.processingtime", System.nanoTime() - start);
    }

    // If error threshold is reached, then terminate processing.
    if (errorCounter > config.threshold) {
      throw new Exception(String.format("Error threshold reached %ld", config.threshold));
    }

    for (StructuredRecord record : records) {
      StructuredRecord.Builder builder = StructuredRecord.builder(oSchema);
      // Iterate through output schema, if the 'record' doesn't have it, then
      // attempt to take if from 'input'.
      for (Schema.Field field : oSchema.getFields()) {
        Object rObject = record.get(field.getName());
        Object iObject = input.get(field.getName());
        if (rObject == null) {
          builder.convertAndSet(field.getName(), (String) iObject);
        } else {
          if (rObject instanceof String && Strings.isNullOrEmpty((String) rObject)) {
            builder.set(field.getName(), null);
          } else {
            builder.set(field.getName(), rObject);
          }
        }

      }
      emitter.emit(builder.build());
    }
  }

  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {
    @Name("specification")
    @Description("Directives for wrangling the input records")
    private String specification;

    @Name("field")
    @Description("Name of the input field to be wrangled or '*' to wrangle all the fields.")
    private final String field;

    @Name("threshold")
    @Description("Max number of event failures in wrangling after which to stop the pipeline of processing." +
      "Threshold is not aggregate across all instance, but is applied for each running instances")
    private final int threshold;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String specification, String field, int threshold, String schema) {
      this.specification = specification;
      this.field = field;
      this.threshold = threshold;
      this.schema = schema;
    }
  }
}

