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
package io.cdap.directives.aggregates;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;


@Plugin(type = Directive.TYPE)
@Name("AggregateSizeAndTime")
@Categories(categories = {"transient"})
@Description("Aggregates size and time columns with optional unit conversions and average support.")
public class AggregateSizeAndTime implements Directive {

    private String sizeCol;
    private String timeCol;
    private String totalSizeCol;
    private String totalTimeCol;
    private String sizeUnit = "MB";
    private String timeUnit = "s";
    private String timeAgg = "total";

    public AggregateSizeAndTime() {
    }

    @Override
    public UsageDefinition define() {
      UsageDefinition.Builder builder = UsageDefinition.builder("AggregateSizeAndTime");
    
      // Required arguments
      builder.define("sizeCol", TokenType.IDENTIFIER);       // Source column for byte sizes
      builder.define("timeCol", TokenType.IDENTIFIER);       // Source column for durations
      builder.define("totalSizeCol", TokenType.IDENTIFIER);  // Target column for total size
      builder.define("totalTimeCol", TokenType.IDENTIFIER);  // Target column for total/avg time
    
      // Optional arguments
      builder.define("sizeUnit", TokenType.TEXT);            // "MB", "GB", etc.
      builder.define("timeUnit", TokenType.TEXT);            // "s", "min", etc.
      builder.define("timeAgg", TokenType.TEXT);             // "total" or "average"
    
      return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeCol = ((Identifier) args.value("sizeCol")).value();
        this.timeCol = ((Identifier) args.value("timeCol")).value();
        this.totalSizeCol = ((Identifier) args.value("totalSizeCol")).value();
        this.totalTimeCol = ((Identifier) args.value("totalTimeCol")).value();
    
        if (args.contains("sizeUnit")) {
            this.sizeUnit = ((Text) args.value("sizeUnit")).value().toUpperCase();
        }
    
        if (args.contains("timeUnit")) {
            this.timeUnit = ((Text) args.value("timeUnit")).value().toLowerCase();
        }
    
        if (args.contains("timeAgg")) {
            this.timeAgg = ((Text) args.value("timeAgg")).value().toLowerCase();
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
      TransientStore store = context.getTransientStore();
    
      for (Row row : rows) {
        Object sizeObj = row.getValue(sizeCol);
        Object timeObj = row.getValue(timeCol);
    
        // Skip if missing/null values
        if (sizeObj == null || timeObj == null) {
          continue;
        }
    
        try {
          // Convert input size to bytes
          long sizeInBytes = convertToBytes(sizeObj.toString(), sizeUnit);
    
          // Convert input time to nanoseconds
          long timeInNanos = convertToNanos(timeObj.toString(), timeUnit);
    
          // Accumulate in the transient store using GLOBAL scope
          store.increment(TransientVariableScope.GLOBAL, "agg_size_bytes", sizeInBytes);
          store.increment(TransientVariableScope.GLOBAL, "agg_time_nanos", timeInNanos);
          store.increment(TransientVariableScope.GLOBAL, "agg_row_count", 1L);
        } catch (Exception e) {
          throw new DirectiveExecutionException("AggregateSizeAndTime", e.getMessage(), e);
        }
      }
    
      return Collections.emptyList(); // Final output generated in finalize()
    }

public List<Row> finalize(TransientStore store) {
    List<Row> output = new ArrayList<>();

    Long sizeBytes = store.get("GLOBAL:agg_size_bytes");
    Long timeNanos = store.get("GLOBAL:agg_time_nanos");
    Long rowCount = store.get("GLOBAL:agg_row_count");

    if (sizeBytes == null) sizeBytes = 0L;
    if (timeNanos == null) timeNanos = 0L;
    if (rowCount == null) rowCount = 0L;

    // convert back to target units
    double totalSize = convertFromBytes(sizeBytes, sizeUnit);
    double finalTime;

    if ("average".equals(timeAgg)) {
        finalTime = convertFromNanos(rowCount == 0 ? 0.0 : timeNanos / (double) rowCount, timeUnit);
    } else {
        finalTime = convertFromNanos((double) timeNanos, timeUnit);
    }    

    Row result = new Row();
    result.add(totalSizeCol, totalSize);
    result.add(totalTimeCol, finalTime);
    output.add(result);

    return output;
}


    @Override
    public void destroy() {
        // Optional cleanup logic if needed
    }

    /**
     * Converts size or time strings to their canonical units: bytes or nanoseconds.
     */
    private long convertToBytes(String value, String unit) {
        double size = Double.parseDouble(value);
        switch (unit.toUpperCase()) {
          case "B":
            return (long) size;
          case "KB":
            return (long) (size * 1024);
          case "MB":
            return (long) (size * 1024 * 1024);
          case "GB":
            return (long) (size * 1024 * 1024 * 1024);
          case "TB":
            return (long) (size * 1024L * 1024 * 1024 * 1024);
          default:
            throw new IllegalArgumentException("Unsupported size unit: " + unit);
        }
    }

    private double convertFromBytes(long value, String unit) {
        switch (unit.toUpperCase()) {
          case "B":
            return value;
          case "KB":
            return value / 1024.0;
          case "MB":
            return value / (1024.0 * 1024);
          case "GB":
            return value / (1024.0 * 1024 * 1024);
          case "TB":
            return value / (1024.0 * 1024 * 1024 * 1024);
          default:
            throw new IllegalArgumentException("Unsupported size unit: " + unit);
        }
    }

    private long convertToNanos(String value, String unit) {
        double time = Double.parseDouble(value);
        switch (unit.toLowerCase()) {
          case "ns":
          case "nanoseconds":
            return (long) time;
          case "us":
          case "microseconds":
            return (long) (time * 1_000);
          case "ms":
          case "milliseconds":
            return (long) (time * 1_000_000);
          case "s":
          case "seconds":
            return (long) (time * 1_000_000_000);
          case "min":
          case "minutes":
            return (long) (time * 60 * 1_000_000_000L);
          case "hr":
          case "hours":
            return (long) (time * 3600 * 1_000_000_000L);
          default:
            throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
    }

    private double convertFromNanos(double value, String unit) {
        switch (unit.toLowerCase()) {
            case "ns":
            case "nanoseconds":
                return value;
            case "us":
            case "microseconds":
                return value / 1_000;
            case "ms":
            case "milliseconds":
                return value / 1_000_000;
            case "s":
            case "seconds":
                return value / 1_000_000_000;
            case "min":
            case "minutes":
                return value / (60 * 1_000_000_000.0);
            case "hr":
            case "hours":
                return value / (3600 * 1_000_000_000.0);
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
    }
}   
