/*
 *  Copyright © 2020 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.datamodel;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.datamodel.HTTPSchemaLoader;
import io.cdap.wrangler.utils.AvroSchemaGlossary;
import io.cdap.wrangler.utils.ColumnConverter;
import org.apache.avro.Schema;

import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A directive for mapping a column to a field within a data model. Mapping entails
 * changing the type of the column to match a target type and changing the name of
 * the column to match a target field name.
 */
@Plugin(type = Directive.TYPE)
@Name(DataModelMapColumn.NAME)
@Categories(categories = {"data-quality"})
@Description("Maps a column to target data model field so that matches the target name and type.")
public class DataModelMapColumn implements Directive, Lineage {

  public static final String NAME = "data-model-map-column";

  private static final String DATA_MODEL = "data-model";
  private static final String DATA_MODEL_REVISION = "revision";
  private static final String MODEL = "model";
  private static final String TARGET_FIELD = "target-field";
  private static final String COLUMN = "column";
  private static final String DATA_MODEL_URL = "data-model-url";
  private static Map<String, AvroSchemaGlossary> glossaryCache = new HashMap<>();

  private String column;
  private String targetFieldName;
  private String targetFieldTypeName;

  // Used for testing purposes only.
  static void setGlossary(String key, AvroSchemaGlossary glossary) {
    glossaryCache.put(key, glossary);
  }

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define(DATA_MODEL_URL, TokenType.TEXT);
    builder.define(DATA_MODEL, TokenType.TEXT);
    builder.define(DATA_MODEL_REVISION, TokenType.NUMERIC);
    builder.define(MODEL, TokenType.TEXT);
    builder.define(TARGET_FIELD, TokenType.TEXT);
    builder.define(COLUMN, TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    String dataModelUrl = ((Text) args.value(DATA_MODEL_URL)).value();
    if (!glossaryCache.containsKey(dataModelUrl)) {
      AvroSchemaGlossary glossary = new AvroSchemaGlossary(new HTTPSchemaLoader(dataModelUrl, "manifest.json"));
      if (!glossary.configure()) {
        throw new DirectiveParseException(NAME, String.format("Unable to load data models from %s.", dataModelUrl));
      }
      glossaryCache.put(dataModelUrl, glossary);
    }

    String dataModelName = ((Text) args.value(DATA_MODEL)).value();
    long revision = ((Numeric) args.value(DATA_MODEL_REVISION)).value().longValue();
    Schema dataModel = glossaryCache.get(dataModelUrl).get(dataModelName, revision);
    if (dataModel == null) {
      throw new DirectiveParseException(NAME, String
        .format("Unable to find data model %s revision %d.", dataModelName, revision));
    }

    String modelName = ((Text) args.value(MODEL)).value();
    Schema.Field modelField = dataModel.getField(modelName);
    if (modelField == null) {
      throw new DirectiveParseException(NAME, String
        .format("Model %s does not exist in data model %s.", modelName, dataModelName));
    }
    Schema subSchema = modelField.schema();
    if (subSchema == null) {
      throw new DirectiveParseException(NAME, String.format("Model %s has no schema.", modelField.name()));
    }

    Schema model = subSchema.getTypes().stream()
      .filter(s -> s.getType() == Schema.Type.RECORD)
      .findFirst()
      .orElse(null);
    if (model == null) {
      throw new DirectiveParseException(NAME, String.format("Model %s has no schema.", subSchema.getName()));
    }

    String targetName = ((Text) args.value(TARGET_FIELD)).value();
    Schema.Field targetField = model.getField(targetName);
    if (targetField == null) {
      throw new DirectiveParseException(NAME, String
        .format("Field %s does not exist in model %s.", targetName, model.getName()));
    }
    Schema type = targetField.schema().getTypes().stream()
      .filter(s -> s.getType() != Schema.Type.NULL)
      .findFirst()
      .orElse(null);
    if (type == null) {
      throw new DirectiveParseException(NAME,
                                        String
                                          .format(" Field %s of model %s in data model %s is missing type information.",
                                                  targetField.name(), modelName, dataModelName));
    }
    targetFieldName = targetField.name();
    targetFieldTypeName = type.getName();
    column = ((ColumnName) args.value(COLUMN)).value();
  }
  
  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      ColumnConverter.convertType(NAME, row, column, targetFieldTypeName, null, null, RoundingMode.UNNECESSARY);
      ColumnConverter.rename(NAME, row, column, targetFieldName);
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Mapped column '%s' to column name '%s' and type '%s'", column, targetFieldName, targetFieldTypeName)
      .relation(column, targetFieldName)
      .build();
  }
}
