/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldTransformOperation;
import co.cask.directives.aggregates.DefaultTransientStore;
import co.cask.wrangler.api.CompileException;
import co.cask.wrangler.api.CompileStatus;
import co.cask.wrangler.api.Compiler;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveInfo;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.DirectiveRegistry;
import co.cask.wrangler.api.ErrorRecord;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.RecipeSymbol;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.TokenGroup;
import co.cask.wrangler.api.TransientStore;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.ConfigDirectiveContext;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.parser.RecipeCompiler;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import co.cask.wrangler.registry.UserDirectiveRegistry;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Wrangler - A interactive tool for data cleansing and transformation.
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

  // Directive registry.
  private DirectiveRegistry registry;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Wrangler(Config config) {
    this.config = config;
  }

  /**
   * Configures the plugin during deployment of the pipeline that uses the plugin.
   *
   * <p>
   *   <ul>
   *     <li>Parses the directives configured. If there are any issues they will highlighted during deployment</li>
   *     <li>Input schema is validated.</li>
   *     <li>Compiles pre-condition expression.</li>
   *   </ul>
   * </p>
   */
  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);

    try {
      Schema iSchema = configurer.getStageConfigurer().getInputSchema();
      if (!config.containsMacro("field") && !config.field.equalsIgnoreCase("*")) {
        validateInputSchema(iSchema);
      }

      String directives = config.directives;
      if (config.udds != null && !config.udds.trim().isEmpty()) {
        if (config.containsMacro("directives")) {
          directives = String.format("#pragma load-directives %s;", config.udds);
        } else {
          directives = String.format("#pragma load-directives %s;%s", config.udds, config.directives);
        }
      }

      // Validate the DSL by compiling the DSL. In case of macros being
      // specified, the compilation will them at this phase.
      Compiler compiler = new RecipeCompiler();
      try {
        // Compile the directive extracting the loadable plugins (a.k.a
        // Directives in this context).
        CompileStatus status = compiler.compile(new MigrateToV2(directives).migrate());
        RecipeSymbol symbols = status.getSymbols();
        Set<String> dynamicDirectives = symbols.getLoadableDirectives();
        for (String directive : dynamicDirectives) {
          Object o = configurer.usePlugin(Directive.Type, directive, directive, PluginProperties.builder().build());
          if (o == null) {
            throw new IllegalArgumentException(
              String.format("User Defined Directive '%s' is not deployed or is not available.", directive)
            );
          }
        }

        // If the 'directives' contains macro, then we would not attempt to compile
        // it.
        if(!config.containsMacro("directives")) {
          // Create the registry that only interacts with system directives.
          registry = new CompositeDirectiveRegistry(new SystemDirectiveRegistry());

          if (symbols != null) {
            Iterator<TokenGroup> iterator = symbols.iterator();
            while(iterator != null && iterator.hasNext()) {
              TokenGroup group = iterator.next();
              if (group != null) {
                String directive = (String) group.get(0).value();
                DirectiveInfo directiveInfo = registry.get(directive);
                if (directiveInfo == null && !dynamicDirectives.contains(directive)) {
                  throw new IllegalArgumentException(
                    String.format("Wrangler plugin has a directive '%s' that does not exist in system or " +
                                    "user space. Either it is a typographical error or the user directive is " +
                                    "not loaded.", directive)
                  );
                }
              }
            }
          }
        }
      } catch (CompileException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      } catch (DirectiveParseException e) {
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

    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    List<String> inputFields = new ArrayList<>();
    List<String> outputFields = new ArrayList<>();
    Schema inputSchema = context.getInputSchema();
    if (checkSchema(inputSchema, "input")) {
      //noinspection ConstantConditions
      inputFields = inputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    }
    Schema outputSchema = context.getOutputSchema();
    if (checkSchema(outputSchema, "output")) {
      //noinspection ConstantConditions
      outputFields = outputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    }
    FieldOperation dataPrepOperation = new FieldTransformOperation("Prepare Data",
                                                                   new MigrateToV2(config.directives).migrate(),
                                                                   inputFields,
                                                                   outputFields);
    context.record(Collections.singletonList(dataPrepOperation));
  }

  private boolean checkSchema(Schema schema, String name) {
    if (schema == null) {
      LOG.debug(String.format("The %s schema is null. Field level lineage will not be recorded", name));
      return false;
    }
    if (schema.getFields() == null) {
      LOG.debug(String.format("The %s schema fields are null. Field level lineage will not be recorded", name));
      return false;
    }
    return true;
  }

  /**
   * Validates input schema.
   *
   * @param inputSchema configured for the plugin.
   */
  private void validateInputSchema(Schema inputSchema) {
    if (inputSchema != null) {
      // Check the existence of field in input schema
      Schema.Field inputSchemaField = inputSchema.getField(config.field);
      if (inputSchemaField == null) {
        throw new IllegalArgumentException(
          "Field " + config.field + " is not present in the input schema");
      }
    }
  }

  /**
   * Initialize the wrangler by parsing the directives and creating the runtime context.
   *
   * @param context framework context being passed.
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    // Parse DSL and initialize the wrangle pipeline.
    store = new DefaultTransientStore();
    registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(),
      new UserDirectiveRegistry(context)
    );

    String directives = config.directives;
    if (config.udds != null && !config.udds.trim().isEmpty()) {
      directives = String.format("#pragma load-directives %s;%s", config.udds, config.directives);
    }

    RecipeParser recipe = new GrammarBasedParser(new MigrateToV2(directives).migrate(), registry);
    ExecutorContext ctx = new WranglerPipelineContext(ExecutorContext.Environment.TRANSFORM, context, store);

    // Based on the configuration create output schema.
    try {
      oSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Stage:%s - Format of output schema specified is invalid. Please check the format.",
                      context.getStageName())
      );
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
        recipe.initialize(dContext);
      } else {
        // this is normal in cloud environments
        LOG.info(String.format("Stage:%s - The Dataprep service is not accessible in this environment. "
                                 + "No aliasing and restriction will be applied.", getContext().getStageName()));
        recipe.initialize(null);
      }
    } catch (IOException | URISyntaxException e) {
      // If there is a issue, we need to fail the pipeline that has the plugin.
      throw new IllegalArgumentException(
        String.format("Stage:%s - Issue in retrieving the configuration from the service. %s",
                      getContext().getStageName(), e.getMessage())
      );
    }

    try {
      // Create the pipeline executor with context being set.
      pipeline = new RecipePipelineExecutor();
      pipeline.initialize(recipe, ctx);
    } catch (Exception e) {
      throw new Exception(
        String.format("Stage:%s - %s", getContext().getStageName(), e.getMessage())
      );
    }

    // Initialize the error counter.
    errorCounter = 0;
  }


  @Override
  public void destroy() {
    super.destroy();
    pipeline.destroy();
    try {
      registry.close();
    } catch (IOException e) {
      LOG.warn("Unable to close the directive registry. You might see increasing number of open file handle.",
               e.getMessage());
    }
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
          row.add(field.getName(), getValue(input, field.getName()));
        }
      } else if ("#".equalsIgnoreCase(config.field)) {
        row.add(input.getSchema().getRecordName(), input);
      } else {
        row.add(config.field, getValue(input, config.field));
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
        for (ErrorRecord error : errors) {
          emitter.emitError(new InvalidEntry<>(error.getCode(), error.getMessage(), input));
        }
      }

    } catch (Exception e) {
      getContext().getMetrics().count("failures", 1);
      errorCounter++;
      // If error threshold is reached, then terminate processing
      // If threshold is set to -1, it tolerant unlimited errors
      if (config.threshold != -1 && errorCounter > config.threshold) {
        emitter.emitAlert(ImmutableMap.of(
          "stage", getContext().getStageName(),
          "code", String.valueOf(1),
          "message", "Error threshold reached.",
          "value", String.valueOf(errorCounter)
        ));
        if (e instanceof DirectiveExecutionException) {
          throw new Exception(String.format("Stage:%s - Reached error threshold %d, terminating processing " +
                                              "due to error : %s", getContext().getStageName(), config.threshold,
                                            e.getMessage()));

        } else {
          throw new Exception(String.format("Stage:%s - Reached error threshold %d, terminating processing " +
                                              "due to error : %s", getContext().getStageName(), config.threshold,
                                            e.getMessage()), e);
        }
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
            // No need to use specific methods for fields of logical type - timestamp, date and time. This is because
            // the wObject should already have correct values for corresponding primitive types.
            builder.set(field.getName(), wObject);
          }
        }
      }
      emitter.emit(builder.build());
    }
  }

  private Object getValue(StructuredRecord input, String fieldName) {
    Schema fieldSchema = input.getSchema().getField(fieldName).getSchema();
    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return input.getDate(fieldName);
        case TIME_MILLIS:
        case TIME_MICROS:
          return input.getTime(fieldName);
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return input.getTimestamp(fieldName);
        default:
          throw new UnexpectedFormatException("Field type " + logicalType + " is not supported.");
      }
    }

    // If the logical type is present in complex types, it will be retrieved as corresponding
    // simple type (int/long).
    return input.get(fieldName);
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
    @Nullable
    private String directives;

    @Name("udd")
    @Description("List of User Defined Directives (UDD) that have to be loaded.")
    @Nullable
    private String udds;

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

    public Config(String precondition, String directives, String udds,
                  String field, int threshold, String schema) {
      this.precondition = precondition;
      this.directives = directives;
      this.udds = udds;
      this.field = field;
      this.threshold = threshold;
      this.schema = schema;
    }
  }
}

