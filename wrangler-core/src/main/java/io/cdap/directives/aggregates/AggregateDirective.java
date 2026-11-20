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



@Plugin(type = Directive.TYPE)
@Name(AggregateSizeAndTime.NAME)
@Categories(categories = { "aggregate" })
@Description("Aggregates total byte sizes and total time durations over all rows.")
public class AggregateSizeAndTime implements Directive {
  public static final String NAME = "aggregate-size-time";

  private String sizeColumn;
  private String timeColumn;
  private String totalSizeColumn;
  private String totalTimeColumn;
  private String byteUnit;
  private String timeUnit;
  private String aggregationType;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("sizeColumn", TokenType.IDENTIFIER);
    builder.define("timeColumn", TokenType.IDENTIFIER);
    builder.define("totalSizeColumn", TokenType.IDENTIFIER);
    builder.define("totalTimeColumn", TokenType.IDENTIFIER);
    builder.define("byteUnit", TokenType.STRING, true);  // Optional byte unit
    builder.define("timeUnit", TokenType.STRING, true);  // Optional time unit
    builder.define("aggregationType", TokenType.STRING, true);  // Optional aggregation type
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sizeColumn = ((Identifier) args.value("sizeColumn")).value();
    this.timeColumn = ((Identifier) args.value("timeColumn")).value();
    this.totalSizeColumn = ((Identifier) args.value("totalSizeColumn")).value();
    this.totalTimeColumn = ((Identifier) args.value("totalTimeColumn")).value();
    this.byteUnit = (String) args.value("byteUnit");  // Optional
    this.timeUnit = (String) args.value("timeUnit");  // Optional
    this.aggregationType = (String) args.value("aggregationType");  // Optional
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    long totalBytes = 0L;
    long totalMillis = 0L;
    int rowCount = 0;

    for (Row row : rows) {
      Object sizeObj = row.getValue(sizeColumn);
      Object timeObj = row.getValue(timeColumn);

      if (sizeObj != null) {
        try {
          ByteSize byteSize = new ByteSize(sizeObj.toString());
          totalBytes += byteSize.value();
        } catch (Exception e) {
          throw new DirectiveExecutionException(NAME, "Invalid byte size value: " + sizeObj, e);
        }
      }

      if (timeObj != null) {
        try {
          TimeDuration timeDuration = new TimeDuration(timeObj.toString());
          totalMillis += timeDuration.value();
        } catch (Exception e) {
          throw new DirectiveExecutionException(NAME, "Invalid time duration value: " + timeObj, e);
        }
      }

      rowCount++; // Track the number of rows processed
    }

    // Handle aggregation type (total or average)
    if ("average".equalsIgnoreCase(aggregationType) && rowCount > 0) {
      totalBytes /= rowCount;
      totalMillis /= rowCount;
    }

    // Perform unit conversion if required by arguments
    double convertedTotalBytes = convertByteSize(totalBytes, byteUnit);
    double convertedTotalTime = convertTime(totalMillis, timeUnit);

    // Return a single new Row containing aggregate values
    List<Row> result = new ArrayList<>();
    Row output = new Row();
    output.add(totalSizeColumn, convertedTotalBytes);
    output.add(totalTimeColumn, convertedTotalTime);
    result.add(output);

    return result;
  }

  @Override
  public void destroy() {
    // No resources to clean up
  }

  @Override
  public List<EntityCountMetric> getCountMetrics() {
    return ImmutableList.of();
  }

  // ===== Conversion utilities (inside the class) =====

  private static double convertByteSize(long totalBytes, String targetUnit) {
    if (targetUnit == null || targetUnit.isEmpty()) {
      return (double) totalBytes;
    }

    switch (targetUnit.toUpperCase()) {
      case "B":
        return (double) totalBytes;
      case "KB":
        return totalBytes / 1024.0;
      case "MB":
        return totalBytes / (1024.0 * 1024);
      case "GB":
        return totalBytes / (1024.0 * 1024 * 1024);
      case "TB":
        return totalBytes / (1024.0 * 1024 * 1024 * 1024);
      default:
        throw new IllegalArgumentException("Unsupported target byte unit: " + targetUnit);
    }
  }

  private static double convertTime(long totalNanos, String targetUnit) {
    if (targetUnit == null || targetUnit.isEmpty()) {
      return (double) totalNanos;
    }

    switch (targetUnit.toLowerCase()) {
      case "ns":
        return (double) totalNanos;
      case "us":
      case "microseconds":
        return totalNanos / 1_000.0;
      case "ms":
      case "milliseconds":
        return totalNanos / 1_000_000.0;
      case "s":
      case "seconds":
        return totalNanos / 1_000_000_000.0;
      case "min":
      case "minutes":
        return totalNanos / (60.0 * 1_000_000_000.0);
      case "h":
      case "hours":
        return totalNanos / (3600.0 * 1_000_000_000.0);
      default:
        throw new IllegalArgumentException("Unsupported target time unit: " + targetUnit);
    }
  }
}
