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

/**
 * Parses a byte size string like "10KB", "1.5MB", "2GB", etc.
 */
public class ByteSize {
  private final long bytes;

  public ByteSize(String value) {
    this.bytes = parseByteSize(value);
  }

  public long getBytes() {
    return bytes;
  }

  private long parseByteSize(String value) {
    String trimmed = value.trim().toUpperCase(Locale.ENGLISH);
    double number;
    String unit;

    int index = 0;
    while (index < trimmed.length() && 
           (Character.isDigit(trimmed.charAt(index)) || trimmed.charAt(index) == '.' || trimmed.charAt(index) == '-')) {
      index++;
    }

    if (index == 0) {
      throw new IllegalArgumentException("No numeric value found in byte size: " + value);
    }

    number = Double.parseDouble(trimmed.substring(0, index));
    unit = trimmed.substring(index).trim();

    switch (unit) {
      case "B":
      case "":
        return (long) number;
      case "KB":
        return (long) (number * 1024);
      case "MB":
        return (long) (number * 1024 * 1024);
      case "GB":
        return (long) (number * 1024 * 1024 * 1024);
      case "TB":
        return (long) (number * 1024L * 1024 * 1024 * 1024);
      case "PB":
        return (long) (number * 1024L * 1024 * 1024 * 1024 * 1024);
      case "EB":
        return (long) (number * 1024L * 1024 * 1024 * 1024 * 1024 * 1024);
      default:
        throw new IllegalArgumentException("Unknown byte size unit: " + unit);
    }
  }

  @Override
  public String toString() {
    return bytes + " bytes";
  }
}
