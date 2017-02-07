/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.internal.statistics;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.statistics.ColumnMetric;
import co.cask.wrangler.api.statistics.Statistics;
import io.dataapps.chlorine.finder.FinderEngine;

import java.util.List;
import java.util.Map;

/**
 * Created by nitin on 2/4/17.
 */
public class BasicStatistics implements Statistics {
  private final FinderEngine engine;

  public BasicStatistics() throws Exception {
    engine = new FinderEngine("wrangler-finder.xml", true, false);
  }

  @Override
  public Record aggregate(List<Record> records) {
    ColumnMetric types = new ColumnMetric();
    ColumnMetric stats = new ColumnMetric();

    Double count = new Double(0);
    Record invalid = new Record();
    for (Record record : records) {
      ++count;
      for (int i = 0; i < record.length(); ++i) {
        String column = record.getColumn(i);
        Object object = record.getValue(i);

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
            Map<String, List<String>> finds = engine.findWithType(value);
            for (String find : finds.keySet()) {
              types.increment(column, find);
            }
          }
        }
      }
    }

    Record recordTypes = new Record();
    for (String column : types.getColumns()) {
      recordTypes.add(column, types.percentage(column, count));
    }

    Record recordStats = new Record();
    for (String column : stats.getColumns()) {
      recordStats.add(column, stats.percentage(column, count));
    }

    Record record = new Record();
    record.add("types", recordTypes);
    record.add("stats", recordStats);
    record.add("total", count);

    return record;
  }
}
