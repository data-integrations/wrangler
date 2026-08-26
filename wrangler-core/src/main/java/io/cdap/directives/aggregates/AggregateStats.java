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

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.TimeDuration;

import java.util.List;
import java.util.ArrayList;

public class AggregateStats implements Directive {
  private String byteSizeColumn;
  private String timeDurationColumn;

  private long totalBytes = 0;
  private long totalNanoseconds = 0;
  private int rowCount = 0;
  private String NAME = "aggregate-stats";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("byte-size-column", TokenType.COLUMN_NAME);
    builder.define("time-duration-column", TokenType.COLUMN_NAME);
    builder.define("output-size-column", TokenType.COLUMN_NAME);
    builder.define("output-time-column", TokenType.COLUMN_NAME);
    builder.define("size-unit", TokenType.TEXT, "MB"); // Optional, default is MB
    builder.define("time-unit", TokenType.TEXT, "SEC"); // Optional, default is seconds
    builder.define("method", TokenType.TEXT, "TOTAL"); // Optional, default is total
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) {
    this.byteSizeColumn = ((ColumnName) args.value("byte-size-column")).value();
    this.timeDurationColumn = ((ColumnName) args.value("time-duration-column")).value();

    this.totalBytes = 0;
    this.totalNanoseconds = 0;
    this.rowCount = 0;
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) {

    for (Row row : rows) {
      if (row.getValue(byteSizeColumn) != null) {
        Object sizeObj = row.getValue(byteSizeColumn);
        long bytes = extractBytes(sizeObj);
        totalBytes += bytes;
      }
      if (row.getValue(timeDurationColumn) != null) {
        Object timeObj = row.getValue(timeDurationColumn);
        long nanos = extractNanoseconds(timeObj);
        totalNanoseconds += nanos;
      }

      rowCount++;
    }

    return new ArrayList<>();
  }

  private long extractBytes(Object obj) {
    if (obj instanceof ByteSize) {
      return ((ByteSize) obj).getBytes();
    } else if (obj instanceof Number) {
      return ((Number) obj).longValue();
    } else if (obj instanceof String) {
      try {
        ByteSize byteSize = new ByteSize((String) obj);
        return byteSize.getBytes();
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse byte size from: " + obj);
      }
    }
    throw new IllegalArgumentException("Unsupported type for byte size: " + obj.getClass().getName());
  }

  private long extractNanoseconds(Object obj) {
    if (obj instanceof TimeDuration) {
      return ((TimeDuration) obj).value();
    } else if (obj instanceof Number) {
      return ((Number) obj).longValue();
    } else if (obj instanceof String) {
      try {
        TimeDuration duration = new TimeDuration((String) obj);
        return duration.value();
      } catch (Exception e) {
        try {
          return Long.parseLong((String) obj);
        } catch (NumberFormatException ex) {
          throw new IllegalArgumentException("Unable to parse time duration from: " + obj);
        }
      }
    }
    throw new IllegalArgumentException("Unsupported type for time duration: " + obj.getClass().getName());
  }

  @Override
  public void destroy() {
    // TODO Auto-generated method stub
    return;
  }

  public String getByteSizeColumn() {
    return byteSizeColumn;
  }

  public String getTimeDurationColumn() {
    return timeDurationColumn;
  }

  public long getTotalBytes() {
    return totalBytes;
  }
  public String name(){
    return this.NAME;
  }

  public long getTotalNanoseconds() {
    return totalNanoseconds;
  }

  public int getRowCount() {
    return rowCount;
  }
}