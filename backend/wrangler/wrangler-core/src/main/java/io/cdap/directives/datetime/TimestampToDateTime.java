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
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Directive for parsing a timestamp column as DateTime
 */
@Plugin(type = Directive.TYPE)
@Name("timestamp-to-datetime")
@Categories(categories = {"datetime"})
@Description("Convert a timestamp column to datetime")
public class TimestampToDateTime implements Directive, Lineage {

  public static final String NAME = "timestamp-to-datetime";
  private static final String COLUMN = "column";
  private String column;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define(COLUMN, TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) {
    this.column = ((ColumnName) args.value(COLUMN)).value();
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws ErrorRowException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        continue;
      }
      Object value = row.getValue(idx);
      // If the data in the cell is null or is already Datetime , then skip this row.
      if (value == null || value instanceof LocalDateTime) {
        continue;
      }

      if (!(value instanceof ZonedDateTime)) {
        throw new ErrorRowException(NAME, String.format("Value %s for column %s expected to be timestamp but found %s",
                                                        value.toString(), column, value.getClass().getSimpleName()), 2);
      }

      ZonedDateTime timestamp = (ZonedDateTime) value;
      row.setValue(idx, timestamp.toLocalDateTime());
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
      .readable("Converted column '%s' from timestamp to datetime", column)
      .relation(column, column)
      .build();
  }
}
