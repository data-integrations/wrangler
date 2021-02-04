/*
 *  Copyright © 2021 Cask Data, Inc.
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
package io.cdap.directives.datetime;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.List;

/**
 * Directive for generating current datetime with the specified zone
 */
@Plugin(type = Directive.TYPE)
@Name("current-datetime")
@Categories(categories = {"datetime"})
@Description("Generates current datetime using the given zone")
public class CurrentDateTime implements Directive, Lineage {

  public static final String NAME = "current-datetime";
  private static final String COLUMN = "column";
  private static final String ZONE = "timezone";
  private static final String UTC = "UTC";
  private String column;
  private String zone;
  private ZoneId zoneId;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define(COLUMN, TokenType.COLUMN_NAME);
    builder.define(ZONE, TokenType.TEXT, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value(COLUMN)).value();
    if (args.value(ZONE) == null) {
      this.zone = UTC;
      this.zoneId = ZoneId.of(UTC);
      return;
    }

    this.zone = args.value(ZONE).value().toString();
    try {
      this.zoneId = ZoneId.of(this.zone);
    } catch (IllegalArgumentException | ZoneRulesException exception) {
      throw new DirectiveParseException(NAME, String.format("Zone '%s' is invalid.", this.zone), exception);
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) {
    for (Row row : rows) {
      row.addOrSet(column, LocalDateTime.now(zoneId));
    }
    return rows;
  }

  @Override
  public void destroy() {
    //no op
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Generated current datetime for column '%s' with zone '%s'", column, zone)
      .relation(column, column)
      .build();
  }
}
