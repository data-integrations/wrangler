/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.service.directive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.features.Feature;
import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRecordBase;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.GrammarMigrator;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.GrammarWalker;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.parser.RecipeCompiler;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.ErrorRecordsException;
import io.cdap.wrangler.proto.workspace.ColumnStatistics;
import io.cdap.wrangler.proto.workspace.ColumnValidationResult;
import io.cdap.wrangler.proto.workspace.WorkspaceValidationResult;
import io.cdap.wrangler.proto.workspace.v2.DirectiveExecutionResponse;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.registry.UserDirectiveRegistry;
import io.cdap.wrangler.schema.TransientStoreKeys;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.statistics.BasicStatistics;
import io.cdap.wrangler.statistics.Statistics;
import io.cdap.wrangler.utils.SchemaConverter;
import io.cdap.wrangler.validator.ColumnNameValidator;
import io.cdap.wrangler.validator.Validator;
import io.cdap.wrangler.validator.ValidatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract handler which contains common logic for v1 and v2 endpoints
 *
 * TODO: CDAP-18015 Refactor and add unit test for methods in this class
 */
public class AbstractDirectiveHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDirectiveHandler.class);
  private static final Gson  GSON = new GsonBuilder().registerTypeAdapterFactory(
    new WranglerDisplaySerializer()).create();

  private static final String NULL_COLUMN_DISPLAY_TYPE = "NULL";
  protected static final String COLUMN_NAME = "body";
  protected static final String RECORD_DELIMITER_HEADER = "recorddelimiter";
  protected static final String DELIMITER_HEADER = "delimiter";
  protected static final TransientStore TRANSIENT_STORE = new DefaultTransientStore();

  protected DirectiveRegistry composite;
  protected boolean schemaManagementEnabled;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    composite = new CompositeDirectiveRegistry(
      SystemDirectiveRegistry.INSTANCE,
      new UserDirectiveRegistry(context)
    );
    schemaManagementEnabled = Feature.WRANGLER_SCHEMA_MANAGEMENT.isEnabled(context);
  }

  /**
   * Closes the resources help by the composite registry.
   */
  @Override
  public void destroy() {
    super.destroy();
    try {
      composite.close();
    } catch (IOException e) {
      // If something bad happens here, you might see a lot of open file handles.
      LOG.warn("Unable to close the directive registry. You might see increasing number of open file handle.", e);
    }
  }

  protected <E extends Exception> List<Row> executeDirectives(
      String namespace,
      List<String> directives,
      List<Row> sample,
      GrammarWalker.Visitor<E> grammarVisitor) throws DirectiveParseException, E, RecipeException {

    if (directives.isEmpty()) {
      return sample;
    }

    GrammarMigrator migrator = new MigrateToV2(directives);
    String recipe = migrator.migrate();

    // Parse and call grammar visitor
    try {
      GrammarWalker walker = new GrammarWalker(new RecipeCompiler(), new ConfigDirectiveContext(DirectiveConfig.EMPTY));
      walker.walk(recipe, grammarVisitor);
    } catch (CompileException e) {
      throw new BadRequestException(e.getMessage(), e);
    }

    RecipeParser parser = new GrammarBasedParser(namespace, recipe, composite,
                                                 new ConfigDirectiveContext(DirectiveConfig.EMPTY));
    try (RecipePipelineExecutor executor = new RecipePipelineExecutor(parser,
                                                                      new ServicePipelineContext(
                                                                        namespace, ExecutorContext.Environment.SERVICE,
                                                                        getContext(), TRANSIENT_STORE))) {
      List<Row> result = executor.execute(sample);

      List<ErrorRecordBase> errors = executor.errors()
        .stream()
        .filter(ErrorRecordBase::isShownInWrangler)
        .collect(Collectors.toList());

      if (!errors.isEmpty()) {
        throw new ErrorRecordsException(errors);
      }
      return result;
    }
  }

  /**
   * Transform the rows to response that is user friendly. Also generates the summary from the rows.
   */
  protected DirectiveExecutionResponse generateExecutionResponse(
    List<Row> rows, int limit) throws Exception {
    List<Map<String, Object>> values = new ArrayList<>(rows.size());
    Map<String, String> types = new LinkedHashMap<>();
    SchemaConverter convertor = new SchemaConverter();

    if (schemaManagementEnabled) {
      Schema outputSchema = TRANSIENT_STORE.get(TransientStoreKeys.OUTPUT_SCHEMA) != null ?
        TRANSIENT_STORE.get(TransientStoreKeys.OUTPUT_SCHEMA) : TRANSIENT_STORE.get(TransientStoreKeys.INPUT_SCHEMA);
      for (Schema.Field field : outputSchema.getFields()) {
        types.put(field.getName(), getColumnDisplayType(field.getSchema()));
      }
    }

    // Iterate through all the new rows.
    for (Row row : rows) {
      // If output array has more than return result values, we terminate.
      if (values.size() >= limit) {
        break;
      }

      Map<String, Object> value = new HashMap<>(row.width());

      // Iterate through all the fields of the row.
      for (Pair<String, Object> field : row.getFields()) {
        String fieldName = field.getFirst();
        Object object = field.getSecond();

        if (object != null) {
          if (!schemaManagementEnabled) {
            Schema schema = convertor.getSchema(object, fieldName);
            types.put(fieldName, schema != null ? getColumnDisplayType(schema) : object.getClass().getSimpleName());
          }

          if ((object instanceof Iterable)
              || (object instanceof Row)) {
            value.put(fieldName, GSON.toJson(object));
          } else {
            if ((object.getClass().getMethod("toString").getDeclaringClass() != Object.class)) {
              value.put(fieldName, object.toString());
            } else {
              value.put(fieldName, WranglerDisplaySerializer.NONDISPLAYABLE_STRING);
            }
          }
        } else {
          value.put(fieldName, null);
          if (!schemaManagementEnabled) {
            types.put(fieldName, NULL_COLUMN_DISPLAY_TYPE);
          }
        }
      }
      values.add(value);
    }
    return new DirectiveExecutionResponse(values, types.keySet(), types, getWorkspaceSummary(rows));
  }

  /**
   * Get the summary for the workspace rows
   */
  protected WorkspaceValidationResult getWorkspaceSummary(List<Row> rows) throws Exception {
    // Validate Column names.
    Validator<String> validator = new ColumnNameValidator();
    validator.initialize();

    // Iterate through columns to value a set
    Set<String> uniqueColumns = new HashSet<>();
    for (Row row : rows) {
      for (int i = 0; i < row.width(); ++i) {
        uniqueColumns.add(row.getColumn(i));
      }
    }

    Map<String, ColumnValidationResult> columnValidationResults = new HashMap<>();
    for (String name : uniqueColumns) {
      try {
        validator.validate(name);
        columnValidationResults.put(name, new ColumnValidationResult(null));
      } catch (ValidatorException e) {
        columnValidationResults.put(name, new ColumnValidationResult(e.getMessage()));
      }
    }

    // Generate General and Type related Statistics for each column.
    Statistics statsGenerator = new BasicStatistics();
    Row summary = statsGenerator.aggregate(rows);

    Row stats = (Row) summary.getValue("stats");
    Row types = (Row) summary.getValue("types");

    List<Pair<String, Object>> fields = stats.getFields();
    Map<String, ColumnStatistics> statistics = new HashMap<>();
    for (Pair<String, Object> field : fields) {
      List<Pair<String, Double>> values = (List<Pair<String, Double>>) field.getSecond();
      Map<String, Float> generalStats = new HashMap<>();
      for (Pair<String, Double> value : values) {
        generalStats.put(value.getFirst(), value.getSecond().floatValue() * 100);
      }
      ColumnStatistics columnStatistics = new ColumnStatistics(generalStats, null);
      statistics.put(field.getFirst(), columnStatistics);
    }

    fields = types.getFields();
    for (Pair<String, Object> field : fields) {
      List<Pair<String, Double>> values = (List<Pair<String, Double>>) field.getSecond();
      Map<String, Float> typeStats = new HashMap<>();
      for (Pair<String, Double> value : values) {
        typeStats.put(value.getFirst(), value.getSecond().floatValue() * 100);
      }
      ColumnStatistics existingStats = statistics.get(field.getFirst());
      Map<String, Float> generalStats = existingStats == null ? null : existingStats.getGeneral();
      statistics.put(field.getFirst(), new ColumnStatistics(generalStats, typeStats));
    }

    return new WorkspaceValidationResult(columnValidationResults, statistics);
  }

  private String getColumnDisplayType(Schema schema) {
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    String type = schema.getLogicalType() == null ? schema.getType().name() : schema.getLogicalType().name();
    // for backward compatibility, make the characters except the first one to lower case
    type = type.substring(0, 1).toUpperCase() + type.substring(1).toLowerCase();
    return type;
  }
}
