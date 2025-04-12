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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.SchemaResolutionContext;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for copying value of one column to another.
 */
@Plugin(type = Directive.TYPE)
@Name(Copy.NAME)
@Categories(categories = { "column"})
@Description("Copies values from a source column into a destination column.")
public class Copy implements Directive, Lineage {
  public static final String NAME = "copy";
  private ColumnName source;
  private ColumnName destination;
  private boolean force = false;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("force", TokenType.BOOLEAN, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.source = args.value("source");
    this.destination = args.value("destination");
    if (args.contains("force")) {
      force = (boolean) args.value("force").value();
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int sidx = row.find(source.value());
      if (sidx == -1) {
        throw new DirectiveExecutionException(NAME, String.format("Column '%s' does not exist.", source.value()));
      }

      int didx = row.find(destination.value());
      // If source and destination are same, then it's a nop.
      if (didx == sidx) {
        continue;
      }

      if (didx == -1) {
        // if destination column doesn't exist then add it.
        row.add(destination.value(), row.getValue(sidx));
      } else {
        // if destination column exists, and force is set to false, then throw exception, else
        // overwrite it.
        if (!force) {
          throw new DirectiveExecutionException(
            NAME, String.format("Destination column '%s' already exists in the row. Use 'force' " +
                                  "option to overwrite the column.", destination.value()));
        }
        row.setValue(didx, row.getValue(sidx));
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Copied value from column '%s' to '%s'", source.value(), destination.value())
      .conditional(source.value(), destination.value())
      .build();
  }

  @Override
  public Schema getOutputSchema(SchemaResolutionContext context) {
    Schema inputSchema = context.getInputSchema();
    List<Schema.Field> outputFields = new ArrayList<>();
    Schema sourceSchema = inputSchema.getField(source.value()).getSchema();
    boolean destinationExists = inputSchema.getField(destination.value()) != null;

    for (Schema.Field field : inputSchema.getFields()) {
      if (force && field.getName().equals(destination.value())) {
        outputFields.add(Schema.Field.of(destination.value(), sourceSchema));
      } else {
        outputFields.add(field);
      }
    }
    if (!destinationExists) {
      outputFields.add(Schema.Field.of(destination.value(), sourceSchema));
    }
    return Schema.recordOf("outputSchema", outputFields);
  }
}
