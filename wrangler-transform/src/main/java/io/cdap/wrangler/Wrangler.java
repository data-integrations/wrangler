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

package io.cdap.wrangler;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.features.Feature;
import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.DirectiveRelationalTransform;
import io.cdap.wrangler.api.EntityCountMetric;
import io.cdap.wrangler.api.ErrorRecord;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.RecipeSymbol;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.lineage.LineageOperations;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.parser.NoOpDirectiveContext;
import io.cdap.wrangler.parser.RecipeCompiler;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.registry.UserDirectiveRegistry;
import io.cdap.wrangler.utils.SqlExpressionGenerator;
import io.cdap.wrangler.utils.StructuredToRowTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static io.cdap.cdap.features.Feature.WRANGLER_FAIL_PIPELINE_FOR_ERROR;
import static io.cdap.wrangler.metrics.Constants.Tags.APP_ENTITY_TYPE;
import static io.cdap.wrangler.metrics.Constants.Tags.APP_ENTITY_TYPE_NAME;

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
public class Wrangler extends Transform<StructuredRecord, StructuredRecord> implements DirectiveRelationalTransform {
  private static final Logger LOG = LoggerFactory.getLogger(Wrangler.class);

  // Configuration specifying the dataprep application and service name.
  private static final String APPLICATION_NAME = "dataprep";
  private static final String SERVICE_NAME = "service";
  private static final String CONFIG_METHOD = "config";
  private static final String ON_ERROR_DEFAULT = "fail-pipeline";
  private static final String ON_ERROR_FAIL_PIPELINE = "fail-pipeline";
  private static final String ON_ERROR_PROCEED = "send-to-error-port";
  private static final String ERROR_STRATEGY_DEFAULT = "wrangler.error.strategy.default";

  // Directive usage metric
  public static final String DIRECTIVE_METRIC_NAME = "wrangler.directive.count";
  public static final int DIRECTIVE_METRIC_COUNT = 1;
  public static final String DIRECTIVE_ENTITY_TYPE = "directive";

  // Precondition languages
  private static final String JEXL = "jexl";
  private static final String SQL = "sql";

  // Sql execution value
  private static final String SQL_ENABLED = "yes";

  // wrangler sql execution mode enabled or not

  public boolean isSqlenabled = false;

  // Plugin configuration.
  private final Config config;

  // Wrangle Execution RecipePipeline
  private RecipePipeline pipeline;

  // Output Schema associated with readable output.
  private Schema oSchema = null;

  // Error counter.
  private long errorCounter;

  // Precondition application
  private Precondition condition = null;

  // Transient Store
  private TransientStore store;

  // Directive registry.
  private DirectiveRegistry registry;

  // on error strategy
  private String onErrorStrategy;

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
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();

    try {
      Schema iSchema = configurer.getStageConfigurer().getInputSchema();
      if (!config.containsMacro(Config.NAME_FIELD) && !(config.getField().equals("*")
        || config.getField().equals("#"))) {
        validateInputSchema(iSchema, collector);
      }

      String directives = config.getDirectives();
      if (config.getUDDs() != null && !config.getUDDs().trim().isEmpty()) {
        if (config.containsMacro("directives")) {
          directives = String.format("#pragma load-directives %s;", config.getUDDs());
        } else {
          directives = String.format("#pragma load-directives %s;%s", config.getUDDs(), config.getDirectives());
        }
      }

      if (!config.containsMacro(Config.NAME_PRECONDITION_LANGUAGE)) {
        if (checkSQLExecution(config)) {
          if (!config.containsMacro(Config.NAME_PRECONDITION_SQL)) {
            validatePrecondition(config.getPreconditionSQL(), true, collector);
          }
          validateSQLUDDs(collector);
        } else {
          if (!config.containsMacro(Config.NAME_PRECONDITION)) {
            validatePrecondition(config.getPreconditionJEXL(), false, collector);
          }
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
        if (symbols != null) {
          Set<String> dynamicDirectives = symbols.getLoadableDirectives();
          for (String directive : dynamicDirectives) {
            Object directivePlugin = configurer.usePlugin(Directive.TYPE, directive, directive,
                                                          PluginProperties.builder().build());
            if (directivePlugin == null) {
              collector.addFailure(
                String.format("User Defined Directive '%s' is not deployed or is not available.", directive),
                "Ensure the directive is deployed.")
                .withPluginNotFound(directive, directive, Directive.TYPE)
                .withConfigElement(Config.NAME_UDD, directive);
            }
          }
          // If the 'directives' contains macro, then we would not attempt to compile
          // it.
          if (!config.containsMacro(Config.NAME_DIRECTIVES)) {
            // Create the registry that only interacts with system directives.
            registry = SystemDirectiveRegistry.INSTANCE;

            Iterator<TokenGroup> iterator = symbols.iterator();
            while (iterator.hasNext()) {
              TokenGroup group = iterator.next();
              if (group != null) {
                String directive = (String) group.get(0).value();
                DirectiveInfo directiveInfo = registry.get("", directive);
                if (directiveInfo == null && !dynamicDirectives.contains(directive)) {
                  collector.addFailure(
                    String.format("Wrangler plugin has a directive '%s' that does not exist in system or " +
                                    "user space.", directive),
                    "Ensure the directive is loaded or the directive name is correct.")
                    .withConfigProperty(Config.NAME_DIRECTIVES);
                }
              }
            }
          }
        }

        // check if the directive is supported by SQL
        if (checkSQLExecution(config)) {
          List<Directive> sqlDirectives = null;
          sqlDirectives = getDirectivesList(config);
          validateSQLModeDirectives(collector, sqlDirectives);
        }

      } catch (CompileException e) {
        collector.addFailure("Compilation error occurred : " + e.getMessage(), null);
      } catch (DirectiveParseException e) {
        collector.addFailure(e.getMessage(), null);
      }

      // Based on the configuration create output schema.
      try {
        if (!config.containsMacro(Config.NAME_SCHEMA)) {
          oSchema = Schema.parseJson(config.schema);
        }
      } catch (IOException e) {
        collector.addFailure("Invalid output schema.", null)
          .withConfigProperty(Config.NAME_SCHEMA).withStacktrace(e.getStackTrace());
      }

      // Check if jexl pre-condition is not null or empty and if so compile expression.
      if (!config.containsMacro(Config.NAME_PRECONDITION) && !config.containsMacro(Config.NAME_PRECONDITION_LANGUAGE)) {
        if (!checkSQLExecution(config) && checkPreconditionNotEmpty(false)) {
          try {
            new Precondition(config.getPreconditionJEXL());
          } catch (PreconditionException e) {
            collector.addFailure(e.getMessage(), null).withConfigProperty(Config.NAME_PRECONDITION);
          }
        }
      }

      // Set the output schema.
      if (oSchema != null) {
        configurer.getStageConfigurer().setOutputSchema(oSchema);
      }

    } catch (Exception e) {
      LOG.error(e.getMessage());
      collector.addFailure("Error occurred : " + e.getMessage(), null).withStacktrace(e.getStackTrace());
    }
  }

  /**
   * {@code prepareRun} is invoked by the client once before the job is submitted, but after the resolution
   * of macros if there are any defined.
   *
   * @param context a instance {@link StageSubmitterContext}
   * @throws Exception thrown if there any issue with prepareRun.
   */
  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);

    // Validate input schema. If there is no input schema available then there
    // is no transformations that can be applied to just return.
    Schema inputSchema = context.getInputSchema();
    if (inputSchema == null || inputSchema.getFields() == null || inputSchema.getFields().isEmpty()) {
      return;
    }

    // After input and output schema are validated, it's time to extract
    // all the fields from input and output schema.
    Set<String> input = inputSchema.getFields().stream()
      .map(Schema.Field::getName).collect(Collectors.toSet());

    // If there is input schema, but if there is no output schema, there is nothing to apply
    // transformations on. So, there is no point in generating field level lineage.
    Schema outputSchema = context.getOutputSchema();
    if (outputSchema == null || outputSchema.getFields() == null || outputSchema.getFields().isEmpty()) {
      return;
    }

    // After input and output schema are validated, it's time to extract
    // all the fields from input and output schema.
    Set<String> output = outputSchema.getFields().stream()
      .map(Schema.Field::getName).collect(Collectors.toSet());

    // Parse the recipe and extract all the instances of directives
    // to be processed for extracting lineage.
    RecipeParser recipe = getRecipeParser(context);
    List<Directive> directives = recipe.parse();
    emitDirectiveMetrics(directives, context.getMetrics());

    LineageOperations lineageOperations = new LineageOperations(input, output, directives);
    context.record(lineageOperations.generate());
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
    RecipeParser recipe = getRecipeParser(context);

    ExecutorContext ctx = new WranglerPipelineContext(ExecutorContext.Environment.TRANSFORM, context, store);

    // Based on the configuration create output schema.
    try {
      oSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Stage:%s - Format of output schema specified is invalid. Please check the format.",
                      context.getStageName()), e
      );
    }
    // initialize the wrangler sql mode
    isSqlenabled = checkSQLExecution(config);
    
    // Check if jexl pre-condition is not null or empty and if so compile expression.
    if (!config.containsMacro(Config.NAME_PRECONDITION_LANGUAGE)) {
      if (!isSqlenabled && checkPreconditionNotEmpty(false)) {
        try {
          condition = new Precondition(config.getPreconditionJEXL());
        } catch (PreconditionException e) {
          throw new IllegalArgumentException(e.getMessage(), e);
        }
      }
    }

    try {
      // Create the pipeline executor with context being set.
      pipeline = new RecipePipelineExecutor(recipe, ctx);
    } catch (Exception e) {
      throw new Exception(String.format("Stage:%s - %s", getContext().getStageName(), e.getMessage()), e);
    }

    String defaultStrategy = context.getArguments().get(ERROR_STRATEGY_DEFAULT);
    onErrorStrategy = (defaultStrategy != null && config.onError == null) ? defaultStrategy : config.getOnError();
    // Initialize the error counter.
    errorCounter = 0;
  }

  @Override
  public void destroy() {
    super.destroy();
    pipeline.close();
    try {
      registry.close();
    } catch (IOException e) {
      LOG.warn("Unable to close the directive registry. You might see increasing number of open file handle.", e);
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
      if ("*".equalsIgnoreCase(config.getField())) {
        row = StructuredToRowTransformer.transform(input);
      } else if ("#".equalsIgnoreCase(config.getField())) {
        row.add(input.getSchema().getRecordName(), input);
      } else {
        row.add(config.getField(), StructuredToRowTransformer.getValue(input, config.getField()));
      }

      // If pre-condition is set, then evaluate the precondition
      if (!isSqlenabled && checkPreconditionNotEmpty(false)) {
        boolean skip = condition.apply(row);
        if (skip) {
          getContext().getMetrics().count("precondition.filtered", 1);
          return; // Expression evaluated to true, so we skip the record.
        }
      }

      // Reset record aggregation store.
      store.reset(TransientVariableScope.GLOBAL);
      store.reset(TransientVariableScope.LOCAL);

      start = System.nanoTime();
      records = pipeline.execute(Collections.singletonList(row), oSchema);
      // We now extract errors from the execution and pass it on to the error emitter.
      List<ErrorRecord> errors = pipeline.errors();
      if (errors.size() > 0) {
        StringJoiner errorMessages = new StringJoiner(",");
        getContext().getMetrics().count("errors", errors.size());
        for (ErrorRecord error : errors) {
          emitter.emitError(new InvalidEntry<>(error.getCode(), error.getMessage(), input));
          errorMessages.add(error.getMessage());
        }
        if (WRANGLER_FAIL_PIPELINE_FOR_ERROR.isEnabled(getContext())
            && onErrorStrategy.equalsIgnoreCase(ON_ERROR_FAIL_PIPELINE)) {
          throw new Exception(
              String.format("Errors in Wrangler Transformation - %s", errorMessages));
        }
      }
    } catch (Exception e) {
      getContext().getMetrics().count("failure", 1);
      if (onErrorStrategy.equalsIgnoreCase(ON_ERROR_PROCEED)) {
        // Emit error record, if the Error flattener or error handlers are not connected, then
        // the record is automatically omitted.
        emitter.emitError(new InvalidEntry<>(0, e.getMessage(), input));
        return;
      }
      if (onErrorStrategy.equalsIgnoreCase(ON_ERROR_FAIL_PIPELINE)) {
        emitter.emitAlert(ImmutableMap.of(
          "stage", getContext().getStageName(),
          "code", String.valueOf(1),
          "message", String.format("Stopping pipeline stage %s on error %s",
                                   getContext().getStageName(), e.getMessage()),
          "value", String.valueOf(errorCounter)
        ));
        throw new Exception(String.format("Stage:%s - Failing pipeline due to error : %s",
                                          getContext().getStageName(), e.getMessage()), e);
      }
      // If it's 'skip-on-error' we continue processing and don't emit any error records.
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

  /**
   * Validates input schema.
   *
   * @param inputSchema configured for the plugin
   * @param collector failure collector
   */
  private void validateInputSchema(@Nullable Schema inputSchema, FailureCollector collector) {
    if (inputSchema != null) {
      // Check the existence of field in input schema
      Schema.Field inputSchemaField = inputSchema.getField(config.getField());
      if (inputSchemaField == null) {
        collector.addFailure(String.format("Field '%s' must be present in input schema.", config.getField()), null)
          .withConfigProperty(Config.NAME_FIELD);
      }
    }
  }

  private void validatePrecondition(String precondition, Boolean isConditionSQL, FailureCollector collector) {
    String field = Config.NAME_PRECONDITION;
    String language = "Precondition (JEXL)";

    if (isConditionSQL == true) {
      field = Config.NAME_PRECONDITION_SQL;
      language = "Precondition (SQL)";
    }
    if (Strings.isNullOrEmpty(precondition)) {
      collector.addFailure(String.format("%s must be present.", language),
                           null)
        .withConfigProperty(field);
    }
  }

  private void validateSQLModeDirectives(FailureCollector collector, List<Directive> directives) {
    for (Directive directive :directives) {
      if (!directive.isSQLSupported()) {
        collector.addFailure(String.format("%s directive is not supported by SQL execution.",
                        directive.define().getDirectiveName()), null)
                .withConfigProperty(Config.NAME_DIRECTIVES);
      }
    }
  }

  private void validateSQLUDDs (FailureCollector collector) {
    if (!Strings.isNullOrEmpty(config.getUDDs())) {
      collector.addFailure("UDDs are not supported for precondition of type SQL", null)
              .withConfigProperty(Config.NAME_UDD);
    }
  }

  private boolean checkPreconditionNotEmpty(Boolean isConditionSQL) {
    if (!isConditionSQL && !Strings.isNullOrEmpty(config.getPreconditionJEXL())
      && !config.getPreconditionJEXL().trim().isEmpty()) {
      return true;
    }
    if (isConditionSQL && !Strings.isNullOrEmpty(config.getPreconditionSQL())
      && !config.getPreconditionSQL().trim().isEmpty()) {
      return true;
    }
    return false;
  }

  private boolean checkSQLExecution(Config config) {
    if (!(Feature.WRANGLER_PRECONDITION_SQL.isEnabled(getContext())
            || Feature.WRANGLER_EXECUTION_SQL.isEnabled(getContext()))) {
      // disabling SQL execution for precondition and directives
      return false;
    }

    if (!Strings.isNullOrEmpty(config.getPreconditionLanguage())) {
      return SQL.equalsIgnoreCase(config.getPreconditionLanguage());
    }

    if (!Strings.isNullOrEmpty(config.getSqlExecution())) {
      return SQL_ENABLED.equalsIgnoreCase(config.getSqlExecution());
    }

    // for backwards compatibility
    return false;
  }

  List<Directive> getDirectivesList(Config config) throws DirectiveParseException, RecipeException {
    String recipe = config.getDirectives();
    List<Directive> directives = null;
    GrammarBasedParser parser = new GrammarBasedParser("default",
            new MigrateToV2(recipe).migrate(), registry);
    directives = parser.parse();
    return directives;
  }

  /**
   * This method creates a {@link CompositeDirectiveRegistry} and initializes the {@link RecipeParser}
   * with {@link NoOpDirectiveContext}
   *
   * @param context
   * @return
   * @throws DirectiveLoadException
   * @throws DirectiveParseException
   */
  private RecipeParser getRecipeParser(StageContext context)
    throws DirectiveLoadException, DirectiveParseException {

    registry = new CompositeDirectiveRegistry(SystemDirectiveRegistry.INSTANCE, new UserDirectiveRegistry(context));
    registry.reload(context.getNamespace());

    String directives = config.getDirectives();
    if (config.getUDDs() != null && !config.getUDDs().trim().isEmpty()) {
      directives = String.format("#pragma load-directives %s;%s", config.getUDDs(), config.getDirectives());
    }

    return new GrammarBasedParser(context.getNamespace(), new MigrateToV2(directives).migrate(), registry);
  }

  @Override
  public Relation transform(RelationalTranformContext relationalTranformContext, Relation relation) {
    if (!checkSQLExecution(config)) {
      return new InvalidRelation("Plugin is not configured for relational transformation");
    }

    Optional<ExpressionFactory<String>> expressionFactory = SqlExpressionGenerator
            .getExpressionFactory(relationalTranformContext);
    if (!expressionFactory.isPresent()) {
      return new InvalidRelation("Cannot find an Expression Factory");
    }

    Expression filterExpression = expressionFactory.get().compile(config.getPreconditionSQL());
    Relation filteredRelation = relation.filter(filterExpression);

    registry = SystemDirectiveRegistry.INSTANCE;
    try {
      registry.reload("default");
    } catch (DirectiveLoadException e) {
      throw new RuntimeException(e);
    }

    List<Directive> directives = null;
    try {
      directives = getDirectivesList(config);
    } catch (DirectiveParseException | RecipeException e) {
      throw new RuntimeException(e);
    }

    for (Directive directive : directives) {
      filteredRelation = directive
          .transform(relationalTranformContext, filteredRelation);
    }
    return filteredRelation;
  }

  private List<String> getColumnsOfDropSQL(String sql) {
    List<String> cols = new ArrayList<>();
    for (String col : sql.split(" ")[2].split(",")) {
      cols.add(col.trim());
    }
    return cols;
  }

  /**
   * This method emits all metrics for the given list of directives
   *
   * @param directives a list of Wrangler directives
   * @param metrics CDAP {@link Metrics} object using which metrics can be emitted
   */
  private void emitDirectiveMetrics(List<Directive> directives, Metrics metrics) throws DirectiveLoadException {
    for (Directive directive : directives) {
      // skip emitting metrics if the directive is not system directive
      if (registry.get(Contexts.SYSTEM, directive.define().getDirectiveName()) == null) {
        continue;
      }
      List<EntityCountMetric> countMetrics = new ArrayList<>();

      // add usage metric
      countMetrics.add(getDirectiveUsageMetric(directive.define().getDirectiveName()));

      // add custom directive metrics
      if (directive.getCountMetrics() != null) {
        countMetrics.addAll(directive.getCountMetrics());
      }

      for (EntityCountMetric countMetric : countMetrics) {
        Metrics child = metrics.child(getEntityMetricTags(countMetric));
        child.countLong(countMetric.getName(), countMetric.getCount());
      }
    }
  }

  private EntityCountMetric getDirectiveUsageMetric(String directiveName) {
    return new EntityCountMetric(
      DIRECTIVE_METRIC_NAME, DIRECTIVE_ENTITY_TYPE, directiveName, DIRECTIVE_METRIC_COUNT);
  }

  private Map<String, String> getEntityMetricTags(EntityCountMetric metricDef) {
    Map<String, String> tags = new HashMap<>();
    tags.put(APP_ENTITY_TYPE, metricDef.getAppEntityType());
    tags.put(APP_ENTITY_TYPE_NAME, metricDef.getAppEntityTypeName());
    return tags;
  }

  /**
   * Config for the plugin.
   */
  public static class Config extends PluginConfig {
    static final String NAME_PRECONDITION = "precondition";
    static final String NAME_PRECONDITION_SQL = "preconditionSQL";
    static final String NAME_PRECONDITION_LANGUAGE = "expressionLanguage";
    static final String NAME_SQL_EXECUTION = "sqlExecution";
    static final String NAME_FIELD = "field";
    static final String NAME_DIRECTIVES = "directives";
    static final String NAME_RELATIONAL_DIRECTIVES = "relationalDirectives";
    static final String NAME_UDD = "udd";
    static final String NAME_SCHEMA = "schema";
    static final String NAME_ON_ERROR = "on-error";

    @Name(NAME_PRECONDITION_LANGUAGE)
    @Description("Toggle to configure precondition language between JEXL and SQL")
    @Macro
    @Nullable
    private String preconditionLanguage;

    @Name(NAME_SQL_EXECUTION)
    @Description("Toggle to configure execution language between JEXL and SQL")
    @Macro
    @Nullable
    private String sqlExecution;

    @Name(NAME_PRECONDITION)
    @Description("JEXL Precondition expression specifying filtering before applying directives (true to filter)")
    @Macro
    @Nullable
    private String precondition;


    @Name(NAME_PRECONDITION_SQL)
    @Description("SQL Precondition expression specifying filtering before applying directives (false to filter)")
    @Macro
    @Nullable
    private String preconditionSQL;


    @Name(NAME_DIRECTIVES)
    @Description("Recipe for wrangling the input records")
    @Macro
    @Nullable
    private String directives;


    @Name(NAME_UDD)
    @Description("List of User Defined Directives (UDD) that have to be loaded.")
    @Nullable
    private String udds;

    @Name(NAME_FIELD)
    @Description("Name of the input field to be wrangled or '*' to wrangle all the fields.")
    @Macro
    private final String field;

    @Name(NAME_SCHEMA)
    @Description("Specifies the schema that has to be output.")
    @Macro
    private final String schema;

    @Name(NAME_ON_ERROR)
    @Description("How to handle error in record processing")
    @Macro
    @Nullable
    private final String onError;

    public Config(String preconditionLanguage, String sqlExecution, String precondition, String directives,
                  String udds, String field, String schema, String onError, String relationalDirectives) {
      this.preconditionLanguage = preconditionLanguage;
      this.sqlExecution = sqlExecution;
      this.precondition = precondition;
      this.directives = directives;
      this.udds = udds;
      this.preconditionSQL = precondition;
      this.field = field;
      this.schema = schema;
      this.onError = onError;
    }

    /**
     * @return if on-error is not specified returns default, else value.
     */
    public String getOnError() {
      return onError == null ? ON_ERROR_DEFAULT : onError;
    }

    public String getPreconditionLanguage() {
      return preconditionLanguage;
    }
    public String getSqlExecution() {
      return sqlExecution;
    }

    public String getPreconditionJEXL() {
      return precondition;
    }

    public String getPreconditionSQL() {
      return preconditionSQL;
    }

    public String getField() {
      return field;
    }

    public String getDirectives() {
      return directives;
    }

    public String getUDDs() {
      return udds;
    }
  }
}
