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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.ErrorRecord;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final Logger LOG = LoggerFactory.getLogger(Wrangler.class);
  // Plugin configuration.
  private final Config config;

  // Wrangle Execution Pipeline
  private Pipeline pipeline;

  // Output Schema associated with transform output.
  private Schema oSchema;

  // Error counter.
  private long errorCounter;

  // Precondition application
  private Precondition condition = null;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Wrangler(Config config) {
    this.config = config;
  }

  /**
   *
   * @param configurer
   * @throws IllegalArgumentException
   */
  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);

    Schema iSchema = configurer.getStageConfigurer().getInputSchema();

    if (!config.field.equalsIgnoreCase("*")) {
      validateInputSchema(iSchema);
    }

    // Validate the DSL by parsing DSL.
    Directives directives = new TextDirectives(config.directives);
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

    // Check if configured field is present in the input schema.
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    if(!config.field.equalsIgnoreCase("*") && inputSchema.getField(config.field) == null) {
      throw new IllegalArgumentException(
        String.format("Field '%s' configured to wrangler is not present in the input. " +
                        "Only specify fields present in the input", config.field)
      );
    }

    // Check if pre-condition is not null or empty and if so compile expression.
    if (config.precondition != null && !config.precondition.trim().isEmpty()) {
      try {
        new Precondition(config.precondition);
      } catch (PreconditionException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    // Set the output schema.
    configurer.getStageConfigurer().setOutputSchema(oSchema);
  }

  /**
   *
   * @param inputSchema
   */
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

  /**
   *
   * @param context
   * @throws Exception
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    // Parse DSL and initialize the wrangle pipeline.
    Directives directives = new TextDirectives(config.directives);
    PipelineContext ctx = new WranglerPipelineContext(PipelineContext.Environment.TRANSFORM, context);

    // Create the pipeline executor with context being set.
    pipeline = new PipelineExecutor();
    pipeline.configure(directives, ctx);

    // Based on the configuration create output schema.
    try {
      oSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of output schema specified is invalid. Please check the format.");
    }

    // Check if pre-condition is not null or empty and if so compile expression.
    if (config.precondition != null && !config.precondition.trim().isEmpty()) {
      try {
        condition = new Precondition(config.precondition);
      } catch (PreconditionException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    // Initialize the error counter.
    errorCounter = 0;
  }

  /**
   *
   * @param input
   * @param emitter
   * @throws Exception
   */
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
      row = new Record(config.field, input.get(config.field));
    }

    // If pre-condition is set, then evaluate the precondition
    if (condition != null) {
      boolean skip = condition.apply(row);
      if (skip) {
        getContext().getMetrics().count("precondition.filtered", 1);
        return; // Expression evaluated to true, so we skip the record.
      }
    }

    // Run through the wrangle pipeline, if there is a SkipRecord exception, don't proceed further
    // but just return without emitting any record out.
    List<StructuredRecord> records = new ArrayList<>();
    long start = System.nanoTime();
    try {
      records = pipeline.execute(Arrays.asList(row), oSchema); // we don't compute meta and statistics.

      // We now extract errors from the execution and pass it on to the error emitter.
      List<ErrorRecord> errors = pipeline.errors();
      if (errors.size() > 0) {
        getContext().getMetrics().count("errors", 1);
        ErrorRecord error = errors.get(0);
        emitter.emitError(new InvalidEntry<StructuredRecord>(error.getCode(), error.getMessage(), input));
      }
    } catch (PipelineException e) {
      getContext().getMetrics().count("failures", 1);
      errorCounter++;
      // If error threshold is reached, then terminate processing.
      if (errorCounter > config.threshold) {
        LOG.warn("Error threshold reached '{}' : {}", config.threshold, e.getMessage());
        throw new Exception(String.format("Reached error threshold %d, terminating processing.", config.threshold));
      }
      // Emit error record, if the Error flattener or error handlers are not connected, then
      // the record is automatically omitted.
      emitter.emitError(new InvalidEntry<StructuredRecord>(0, e.getMessage(), input));
      return;
    } finally {
      getContext().getMetrics().gauge("process.time", System.nanoTime() - start);
    }

    for (StructuredRecord record : records) {
      StructuredRecord.Builder builder = StructuredRecord.builder(oSchema);
      // Iterate through output schema, if the 'record' doesn't have it, then
      // attempt to take if from 'input'.
      for (Schema.Field field : oSchema.getFields()) {
        Object wObject = record.get(field.getName()); // wrangled records
        if(wObject == null) {
          builder.set(field.getName(), null);
        } else {
          if (wObject instanceof String) {
            builder.convertAndSet(field.getName(), (String) wObject);
          } else {
            builder.set(field.getName(), wObject);
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
    @Name("precondition")
    @Description("Precondition expression specifying filtering before applying directives (true to filter)")
    @Macro
    private String precondition;

    @Name("directives")
    @Description("Directives for wrangling the input records")
    private String directives;

    @Name("field")
    @Description("Name of the input field to be wrangled or '*' to wrangle all the fields.")
    @Macro
    private final String field;

    @Name("threshold")
    @Description("Max number of event failures in wrangling after which to stop the pipeline of processing." +
      "Threshold is not aggregate across all instance, but is applied for each running instances")
    @Macro
    private final int threshold;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String precondition, String directives, String field, int threshold, String schema) {
      this.precondition = precondition;
      this.directives = directives;
      this.field = field;
      this.threshold = threshold;
      this.schema = schema;
    }
  }
}

