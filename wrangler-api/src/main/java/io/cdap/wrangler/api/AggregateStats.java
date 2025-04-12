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

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;

import java.util.Collections;
import java.util.List;

/**
 * Directive that aggregates byte sizes and time durations across rows.
 */
public class AggregateStats implements Directive {
  private String sourceSizeCol;
  private String sourceTimeCol;
  private String targetSizeCol;
  private String targetTimeCol;
  private String sizeUnit = "MB";
  private String timeUnit = "seconds";
  private String aggregationType = "total";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("aggregatestats");
    builder.define("sourceSizeCol", TokenType.COLUMN_NAME);
    builder.define("sourceTimeCol", TokenType.COLUMN_NAME);
    builder.define("targetSizeCol", TokenType.COLUMN_NAME);
    builder.define("targetTimeCol", TokenType.COLUMN_NAME);
    builder.define("sizeUnit", TokenType.TEXT, true);
    builder.define("timeUnit", TokenType.TEXT, true);
    builder.define("aggregationType", TokenType.TEXT, true);
    return builder.build();
  }

  @Override
  public void initialize(Arguments arguments) throws DirectiveParseException {
    sourceSizeCol = arguments.value("sourceSizeCol");
    sourceTimeCol = arguments.value("sourceTimeCol");
    targetSizeCol = arguments.value("targetSizeCol");
    targetTimeCol = arguments.value("targetTimeCol");

    if (arguments.contains("sizeUnit")) {
      sizeUnit = arguments.value("sizeUnit");
    }
    if (arguments.contains("timeUnit")) {
      timeUnit = arguments.value("timeUnit");
    }
    if (arguments.contains("aggregationType")) {
      aggregationType = arguments.value("aggregationType");
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    TransientStore store = context.getTransientStore();

    double totalSize = store.getVariables().contains("totalSize") ? store.get("totalSize") : 0.0;
    double totalTime = store.getVariables().contains("totalTime") ? store.get("totalTime") : 0.0;
    int rowCount = store.getVariables().contains("rowCount") ? store.get("rowCount") : 0;

    for (Row row : rows) {
      try {
        String sizeStr = row.getValue(sourceSizeCol).toString();
        String timeStr = row.getValue(sourceTimeCol).toString();

        double sizeBytes = new ByteSize(sizeStr).getBytes();
        double timeNanos = new TimeDuration(timeStr).getNanoseconds();

        totalSize += sizeBytes;
        totalTime += timeNanos;
        rowCount++;
      } catch (Exception e) {
        throw new DirectiveExecutionException("Error parsing input row: " + e.getMessage(), e);
      }
    }

    store.set(TransientVariableScope.GLOBAL, "totalSize", totalSize);
    store.set(TransientVariableScope.GLOBAL, "totalTime", totalTime);
    store.set(TransientVariableScope.GLOBAL, "rowCount", rowCount);

    return Collections.emptyList(); // Final result emitted in finalize()
  }

  public List<Row> finalizeExecution(ExecutorContext context) {
    TransientStore store = context.getTransientStore();

    double totalSize = store.get("totalSize");
    double totalTime = store.get("totalTime");
    int rowCount = store.get("rowCount");

    double finalSize = convertSize(totalSize, sizeUnit);
    double finalTime = convertTime(totalTime, timeUnit);

    if ("average".equalsIgnoreCase(aggregationType) && rowCount > 0) {
      finalSize /= rowCount;
      finalTime /= rowCount;
    }

    Row result = new Row();
    result.add(targetSizeCol, finalSize);
    result.add(targetTimeCol, finalTime);

    return Collections.singletonList(result);
  }



  private double convertSize(double bytes, String unit) {
    switch (unit.toUpperCase()) {
      case "KB": return bytes / 1024;
      case "MB": return bytes / (1024 * 1024);
      case "GB": return bytes / (1024 * 1024 * 1024);
      default: return bytes;
    }
  }

  private double convertTime(double nanos, String unit) {
    switch (unit.toLowerCase()) {
      case "milliseconds": return nanos / 1_000_000;
      case "seconds": return nanos / 1_000_000_000;
      case "minutes": return nanos / (60 * 1_000_000_000);
      default: return nanos;
    }
  }

  @Override
  public void destroy() {
  }
}
