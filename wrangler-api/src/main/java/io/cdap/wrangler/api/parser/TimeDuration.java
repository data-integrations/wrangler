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



package io.cdap.wrangler.api.parser;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Parses a time duration string like "150ms", "2s", "1.5m", "3h", etc.
 */
public class TimeDuration {
  private final long durationMillis;

  public TimeDuration(String value) {
    this.durationMillis = parseDuration(value);
  }

  public long getDurationMillis() {
    return durationMillis;
  }

  private long parseDuration(String value) {
    String trimmed = value.trim().toLowerCase(Locale.ENGLISH);
    double number;
    String unit;

    int index = 0;
    while (index < trimmed.length() && 
           (Character.isDigit(trimmed.charAt(index)) || trimmed.charAt(index) == '.' || trimmed.charAt(index) == '-')) {
      index++;
    }

    if (index == 0) {
      throw new IllegalArgumentException("No numeric value found in time duration: " + value);
    }

    number = Double.parseDouble(trimmed.substring(0, index));
    unit = trimmed.substring(index).trim();

    switch (unit) {
      case "ms":
        return (long) number;
      case "s":
        return (long) TimeUnit.SECONDS.toMillis((long) number);
      case "m":
        return (long) TimeUnit.MINUTES.toMillis((long) number);
      case "h":
        return (long) TimeUnit.HOURS.toMillis((long) number);
      case "d":
        return (long) TimeUnit.DAYS.toMillis((long) number);
      default:
        throw new IllegalArgumentException("Unknown time duration unit: " + unit);
    }
  }

  @Override
  public String toString() {
    return durationMillis + " ms";
  }
}
