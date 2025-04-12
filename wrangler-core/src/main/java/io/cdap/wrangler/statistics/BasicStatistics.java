/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.statistics;

import io.cdap.wrangler.api.Row;
import io.dataapps.chlorine.finder.FinderEngine;

import java.util.List;
import java.util.Map;

/**
 * Basic class to compute summary from a list of rows
 */
public class BasicStatistics implements Statistics {
  // default time out be 10s
  private static final long TIME_OUT_MILLIS = 10000;
  private final FinderEngine engine;

  public BasicStatistics() throws Exception {
    engine = new FinderEngine("wrangler-finder.xml", true, false);
  }

  @Override
  public Row aggregate(List<Row> rows) {
    ColumnMetric types = new ColumnMetric();
    ColumnMetric stats = new ColumnMetric();

    long startTime = System.currentTimeMillis();
    Double count = new Double(0);
    for (Row row : rows) {
      ++count;
      for (int i = 0; i < row.width(); ++i) {
        String column = row.getColumn(i);
        Object object = row.getValue(i);

        if (object == null) {
          stats.increment(column, "null");
        } else {
          stats.increment(column, "non-null");
        }

        if (object instanceof String) {
          String value = ((String) object);
          if (value.isEmpty()) {
            stats.increment(column, "empty");
          } else {
            // this call is very expensive for string > 2000 characters, took seconds to return
            Map<String, List<String>> finds = engine.findWithType(value);
            for (String find : finds.keySet()) {
              types.increment(column, find);
            }
            // TODO: this is a workaround for CDAP-18262, to proper fix we should revisit this computation logic
            if (System.currentTimeMillis() - startTime > TIME_OUT_MILLIS) {
              break;
            }
          }
        }
      }
    }

    Row rowTypes = new Row();
    for (String column : types.getColumns()) {
      rowTypes.add(column, types.percentage(column, count));
    }

    Row rowStats = new Row();
    for (String column : stats.getColumns()) {
      rowStats.add(column, stats.percentage(column, count));
    }

    Row row = new Row();
    row.add("types", rowTypes);
    row.add("stats", rowStats);
    row.add("total", count);

    return row;
  }
}
