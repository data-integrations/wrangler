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
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.directives.aggregates.DefaultTransientStore;
import co.cask.wrangler.api.CompileException;
import co.cask.wrangler.api.CompileStatus;
import co.cask.wrangler.api.Compiler;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.DirectiveRegistry;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.TransientStore;
import co.cask.wrangler.executor.ErrorRecord;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.ConfigDirectiveContext;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.parser.RecipeCompiler;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import co.cask.wrangler.registry.UserDirectiveRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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

  // Configuration specifying the dataprep application and service name.
  private static final String APPLICATION_NAME = "dataprep";
  private static final String SERVICE_NAME = "service";
  private static final String CONFIG_METHOD = "config";

  // Plugin configuration.
  private final Config config;

  // Wrangle Execution RecipePipeline
  private RecipePipeline pipeline;

  // Output Schema associated with transform output.
  private Schema oSchema = null;

  // Error counter.
  private long errorCounter;

  // Precondition application
  private Precondition condition = null;

  // Transient Store
  private TransientStore store;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Wrangler(Config config) {
    this.config = config;
  }

  /**
   * Configures the plugin during deployment of the pipeline that uses the plugin.
   *
   * <p>
   *   <ul>
   *     <li>Parses the directives configured. If there are any issues they will highlighted duirng deployment</li>
   *     <li>Input schema is validated.</li>
   *     <li>Compiles pre-condition expression.</li>
   *   </ul>
   * </p>
   *
   * @param configurer
   */
  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);

    Schema iSchema = configurer.getStageConfigurer().getInputSchema();
    if (!config.containsMacro("field") && !config.field.equalsIgnoreCase("*")) {
      validateInputSchema(iSchema);
    }

    // Validate the DSL by compiling the DSL. In case of macros being
    // specified, the compilation will them at this phase.
    Compiler compiler = new RecipeCompiler();
    try {
      // Compile the directive extracting the loadable plugins (a.k.a
      // Directives in this context).
      CompileStatus status = compiler.compile(new MigrateToV2(config.directives).migrate());
      Set<String> dynamicDirectives = status.getSymbols().getLoadableDirectives();
      for (String directive : dynamicDirectives) {
        // Add the plugin to the pipeline it's running within.
        configurer.usePlugin(Directive.Type, directive, directive,
                             PluginProperties.builder().build());
      }
    } catch (CompileException e) {
      LOG.error(e.getMessage(), e);
      throw new IllegalArgumentException(e.getMessage(), e);
    } catch (DirectiveParseException e) {
      LOG.error(e.getMessage(), e);
      throw new IllegalArgumentException(e.getMessage());
    }

    // Based on the configuration create output schema.
    try {
      if (!config.containsMacro("schema")) {
        oSchema = Schema.parseJson(config.schema);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of output schema specified is invalid. Please check the format.");
    }

    // Check if configured field is present in the input schema.
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    if (!config.containsMacro("field") && !(config.field.equals("*") || config.field.equals("#") ) &&
      (inputSchema != null && inputSchema.getField(config.field) == null)) {
      throw new IllegalArgumentException(
        String.format("Field '%s' configured to wrangler is not present in the input. " +
                        "Only specify fields present in the input", config.field == null ? "null" : config.field)
      );
    }

    // Check if pre-condition is not null or empty and if so compile expression.
    if(!config.containsMacro("precondition")) {
      if (config.precondition != null && !config.precondition.trim().isEmpty()) {
        try {
          new Precondition(config.precondition);
        } catch (PreconditionException e) {
          throw new IllegalArgumentException(e.getMessage());
        }
      }
    }

    // Set the output schema.
    if (oSchema != null) {
      configurer.getStageConfigurer().setOutputSchema(oSchema);
    }
  }

  /**
   * Validates input schema.
   *
   * @param inputSchema configured for the plugin.
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
   * Initialies the wrangler by parsing the directives and creating the runtime context.
   *
   * @param context framework context being passed.
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    // Parse DSL and initialize the wrangle pipeline.
    store = new DefaultTransientStore();
    DirectiveRegistry registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(),
      new UserDirectiveRegistry(context)
    );

    RecipeParser directives = new GrammarBasedParser(new MigrateToV2(config.directives).migrate(), registry);
    ExecutorContext ctx = new WranglerPipelineContext(ExecutorContext.Environment.TRANSFORM, context, store);

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

    // Make a call to retrieve the directive config and create a context.
    // In this plugin, a call is made to the service at runtime.
    // NOTE: This has to be moved to pre-start-plugin phase so that the information
    // it's not heavily parallelized to crash the service.
    try {
      URL url = getDPServiceURL(CONFIG_METHOD);
      if (url != null) {
        ConfigDirectiveContext dContext = new ConfigDirectiveContext(url);
        directives.initialize(dContext);
      } else {
        directives.initialize(null);
      }
    } catch (IOException | URISyntaxException e) {
      // If there is a issue, we need to fail the pipeline that has the plugin.
      throw new IllegalArgumentException(
        String.format("Issue in retrieving the configuration from the service. %s", e.getMessage())
      );
    }

    // Create the pipeline executor with context being set.
    pipeline = new RecipePipelineExecutor();
    pipeline.configure(directives, ctx);

    // Initialize the error counter.
    errorCounter = 0;
  }

  /**
   * Transforms the input record by applying directives on the record being passed.
   *
   * @param input record to be transformed.
   * @param emitter to collect all the output of the transformation.
   * @throws Exception thrown if there are any issue with the transformation.
   */
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    long start = 0;
    List<StructuredRecord> records;
    try {
      // Creates a row as starting point for input to the pipeline.
      Row row = new Row();
      if ("*".equalsIgnoreCase(config.field)) {
        for (Schema.Field field : input.getSchema().getFields()) {
          row.add(field.getName(), input.get(field.getName()));
        }
      } else if ("#".equalsIgnoreCase(config.field)) {
        row.add(input.getSchema().getRecordName(), input);
      } else {
        row.add(config.field, input.get(config.field));
      }

      // If pre-condition is set, then evaluate the precondition
      if (condition != null) {
        boolean skip = condition.apply(row);
        if (skip) {
          getContext().getMetrics().count("precondition.filtered", 1);
          return; // Expression evaluated to true, so we skip the record.
        }
      }

      // Reset record aggregation store.
      store.reset();

      start = System.nanoTime();
      records = pipeline.execute(Arrays.asList(row), oSchema);
      // We now extract errors from the execution and pass it on to the error emitter.
      List<ErrorRecord> errors = pipeline.errors();
      if (errors.size() > 0) {
        getContext().getMetrics().count("errors", errors.size());
        ErrorRecord error = errors.get(0);
        emitter.emitError(new InvalidEntry<>(error.getCode(), error.getMessage(), input));
      }
    } catch (Exception e) {
      getContext().getMetrics().count("failures", 1);
      errorCounter++;
      // If error threshold is reached, then terminate processing
      // If threshold is set to -1, it tolerant unlimited errors
      if (config.threshold != -1 && errorCounter > config.threshold) {
        LOG.error("Error threshold reached '{}' : {}", config.threshold, e.getMessage());
        throw new Exception(String.format("Reached error threshold %d, terminating processing.", config.threshold));
      }
      // Emit error record, if the Error flattener or error handlers are not connected, then
      // the record is automatically omitted.
      emitter.emitError(new InvalidEntry<>(0, e.getMessage(), input));
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
        if (wObject == null) {
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
   * Retrieves the base url from the context and appends method to value to the final url.
   *
   * @param method to be invoked.
   * @return fully formed url to the method.
     */
  private URL getDPServiceURL(String method) throws URISyntaxException, MalformedURLException {
    URL url = getContext().getServiceURL(APPLICATION_NAME, SERVICE_NAME);
    if (url == null) {
      return null;
    }
    URI uri = url.toURI();
    String path = uri.getPath() + method;
    return uri.resolve(path).toURL();
  }

  /**
   * Config for the plugin.
   */
  public static class Config extends PluginConfig {
    @Name("precondition")
    @Description("Precondition expression specifying filtering before applying directives (true to filter)")
    @Macro
    private String precondition;

    @Name("directives")
    @Description("Recipe for wrangling the input records")
    @Macro
    private String directives;

    @Name("field")
    @Description("Name of the input field to be wrangled or '*' to wrangle all the fields.")
    @Macro
    private final String field;

    @Name("threshold")
    @Description("Max number of event failures in wrangling after which to stop the pipeline of processing. " +
      "Threshold is not aggregate across all instance, but is applied for each running instances. " +
      "Set to -1 to specify unlimited number of acceptable errors.")
    @Macro
    private final int threshold;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    @Macro
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

