/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.wrangler.expression;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive that aggregates byte sizes and time durations.
 */
public class AggregateStatsDirective implements Directive {

  private String byteSizeCol;
  private String durationCol;
  private String resultSizeCol;
  private String resultTimeCol;

  @Override
  public void initialize(DirectiveContext context, List<Token> args) throws Exception {
    this.byteSizeCol = ((Text) args.get(0)).value();
    this.durationCol = ((Text) args.get(1)).value();
    this.resultSizeCol = ((Text) args.get(2)).value();
    this.resultTimeCol = ((Text) args.get(3)).value();
  }

  @Override
  public List<Row> execute(List<Row> rows) throws Exception {
    long totalBytes = 0;
    long totalNanos = 0;

    for (Row row : rows) {
      Object byteSizeVal = row.getValue(byteSizeCol);
      Object durationVal = row.getValue(durationCol);

      if (byteSizeVal instanceof String) {
        totalBytes += ByteSizeParser.parse((String) byteSizeVal);
      }

      if (durationVal instanceof String) {
        totalNanos += TimeDurationParser.parse((String) durationVal);
      }
    }

    List<Row> results = new ArrayList<>();
    Row result = new Row();
    result.add(resultSizeCol, totalBytes);
    result.add(resultTimeCol, totalNanos);
    results.add(result);
    return results;
  }

  @Override
  public String usage() {
    return "aggregate-stats <byte_size_col> <duration_col> <result_size_col> <result_time_col>";
  }

  @Override
  public String description() {
    return "Aggregates byte size and duration columns into canonical units.";
  }

  @Override
  public void destroy() {
    // clean up if needed
  }
}
