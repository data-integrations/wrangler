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
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.TokenType;

import java.util.Collections;
import java.util.List;

/**
 * Directive to aggregate size and time duration columns.
 */
public class AggregateSizeDuration implements Directive {

  private String sourceSizeCol;
  private String sourceTimeCol;
  private String targetSizeCol;
  private String targetTimeCol;
  private String sizeUnit = "B";
  private String timeUnit = "ms";
  private String aggType = "total";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("aggregate_size_duration");
    builder.define("sourceSizeCol", TokenType.COLUMN_NAME);
    builder.define("sourceTimeCol", TokenType.COLUMN_NAME);
    builder.define("targetSizeCol", TokenType.COLUMN_NAME);
    builder.define("targetTimeCol", TokenType.COLUMN_NAME);
    builder.define("sizeUnit", TokenType.TEXT, true);
    builder.define("timeUnit", TokenType.TEXT, true);
    builder.define("aggType", TokenType.TEXT, true);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) {
    sourceSizeCol = args.value("sourceSizeCol").toString();
    sourceTimeCol = args.value("sourceTimeCol").toString();
    targetSizeCol = args.value("targetSizeCol").toString();
    targetTimeCol = args.value("targetTimeCol").toString();

    if (args.contains("sizeUnit")) {
      sizeUnit = args.value("sizeUnit").toString().toUpperCase();
    }

    if (args.value("timeUnit") != null) {
      timeUnit = args.value("timeUnit").toString().toLowerCase();
    }

    if (args.value("aggType") != null) {
      aggType = args.value("aggType").toString().toLowerCase();
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) {
    long totalSize = 0;
    long totalTime = 0;

    for (Row row : rows) {
      Object sizeObj = row.getValue(sourceSizeCol);
      Object timeObj = row.getValue(sourceTimeCol);

      long sizeInBytes = Long.parseLong(new ByteSize(sizeObj.toString()).value());
      long timeInMs = Long.parseLong(new TimeDuration(timeObj.toString()).value());

      totalSize += sizeInBytes;
      totalTime += timeInMs;
    }

    double finalSize = convertSize(totalSize, sizeUnit);
    double finalTime = convertTime(totalTime, timeUnit);

    if (aggType.equals("average") && !rows.isEmpty()) {
      finalSize /= rows.size();
      finalTime /= rows.size();
    }

    Row result = new Row();
    result.add(targetSizeCol, finalSize);
    result.add(targetTimeCol, finalTime);

    return Collections.singletonList(result);
  }

  @Override
  public void destroy() {
    // No cleanup needed
  }

  private double convertSize(long size, String unit) {
    switch (unit) {
      case "KB":
        return size / 1024.0;
      case "MB":
        return size / (1024.0 * 1024);
      case "GB":
        return size / (1024.0 * 1024 * 1024);
      case "TB":
        return size / (1024.0 * 1024 * 1024 * 1024);
      default:
        return size;
    }
  }

  private double convertTime(long time, String unit) {
    switch (unit) {
      case "s":
        return time / 1000.0;
      case "min":
        return time / (60.0 * 1000);
      case "h":
        return time / (60.0 * 60 * 1000);
      default:
        return time;
    }
  }
}
