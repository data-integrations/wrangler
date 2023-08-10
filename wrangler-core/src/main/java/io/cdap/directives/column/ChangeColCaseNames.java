/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.directives.column;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.utils.SqlExpressionGenerator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class <code>ChangeColCaseNames</code> converts the case of the columns
 * to either lower-case or uppercase.
 */
@Plugin(type = Directive.TYPE)
@Name(ChangeColCaseNames.NAME)
@Categories(categories = { "column"})
@Description("Changes the case of column names to either lowercase or uppercase.")
public class ChangeColCaseNames implements Directive, Lineage {
  public static final String NAME = "change-column-case";
  private boolean toLower;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("case", TokenType.IDENTIFIER, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    toLower = true;
    if (args.contains("case")) {
      Identifier identifier = args.value("case");
      String casing = identifier.value();
      if (casing.equalsIgnoreCase("upper") || casing.equalsIgnoreCase("uppercase")) {
        toLower = false;
      }
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      for (int i = 0; i < row.width(); ++i) {
        String name = row.getColumn(i);
        if (toLower) {
          row.setColumn(i, name.toLowerCase());
        } else {
          row.setColumn(i, name.toUpperCase());
        }
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Changed all column names to ", toLower ? "'lowercase'" : "'uppercase'")
      .all(Many.of())
      .build();
  }

  @Override
  public Relation transform(RelationalTranformContext relationalTranformContext,
                            Relation relation) {
    java.util.Optional<ExpressionFactory<String>> expressionFactory = SqlExpressionGenerator
            .getExpressionFactory(relationalTranformContext);
    if (!expressionFactory.isPresent()) {
      return new InvalidRelation("Cannot find an Expression Factory");
    }

    // TODO: handle schema changes in relationalTranformContext for multiple directive execution
    List<String> columnNames = SqlExpressionGenerator.generateColumnNameList(relationalTranformContext);
    Map<String, Expression> colmap = generateColumnCaseMap(columnNames, expressionFactory.get());
    return relation.select(colmap);
  }

  private Map<String, Expression> generateColumnCaseMap(List<String> columns, ExpressionFactory<String> factory) {
    Map<String, Expression> columnExpMap = new LinkedHashMap<>();
    if (toLower) {
      columns.forEach((colName) -> columnExpMap.put(colName.toLowerCase(), factory.compile(colName)));
    } else {
      columns.forEach((colName) -> columnExpMap.put(colName.toUpperCase(), factory.compile(colName)));
    }
    return columnExpMap;
  }

  @Override
  public boolean isSQLSupported() {
    return true;
  }

}

