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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.Specification;
import co.cask.wrangler.internal.DefaultPipeline;
import co.cask.wrangler.internal.TextSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;

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
  private static final Logger LOG = LoggerFactory.getLogger(Wrangler.class);

  // Plugin configuration.
  private final Config config;

  // Wrangle Execution Pipeline
  private Pipeline pipeline;

  // Output Schema associated with transform output.
  private Schema oSchema;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Wrangler(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);

    Schema iSchema = configurer.getStageConfigurer().getInputSchema();
    validateInputSchema(iSchema);

    // Validate the DSL by parsing DSL.
    Specification specification = new TextSpecification(config.specification);
    try {
      specification.getSteps();
    } catch (ParseException e) {
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
    Specification specification = new TextSpecification(config.specification);
    pipeline = new DefaultPipeline();
    pipeline.configure(specification);

    // Based on the configuration create output schema.
    try {
      oSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of output schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // Run through the wrangle pipeline.
    StructuredRecord record = (StructuredRecord)pipeline.execute(input.get(config.field), oSchema);
    StructuredRecord.Builder builder = StructuredRecord.builder(oSchema);

    // Iterate through output schema, if the 'record' doesn't have it, then
    // attempt to take if from 'input'.
    for (Schema.Field field : oSchema.getFields()) {
      Object rObject = record.get(field.getName());
      Object iObject = input.get(field.getName());
      if (rObject == null) {
        builder.convertAndSet(field.getName(), (String) iObject);
      } else {
        builder.convertAndSet(field.getName(), (String) rObject);
      }

    }
    emitter.emit(builder.build());
  }

  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {
    @Name("specification")
    @Description("Specification for wrangling the input records")
    private String specification;

    @Name("field")
    @Description("Specifies the field to wrangled.")
    private final String field;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String specification, String field, String schema) {
      this.specification = specification;
      this.field = field;
      this.schema = schema;
    }
  }
}

