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
 
 
  package io.cdap.wrangler.utils;
 
  import java.util.Locale;
  import java.util.regex.Matcher;
  import java.util.regex.Pattern;
  
  /**
   * Utility class to parse and format byte sizes and time durations.
   */
  public final class UnitConverter {
  
    private UnitConverter() {}
  
    private static final Pattern SIZE_PATTERN = Pattern.compile("(\\d+(\\.\\d+)?)\\s*(B|KB|MB|GB|TB)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d+(\\.\\d+)?)\\s*(ns|ms|s|sec|seconds|m|min|minutes|h|hr|hours)", Pattern.CASE_INSENSITIVE);
  
    // Byte size conversion
    public static long parseByteSize(String sizeStr) {
      if (sizeStr == null) throw new IllegalArgumentException("Byte size string cannot be null");
      Matcher matcher = SIZE_PATTERN.matcher(sizeStr.trim());
      if (!matcher.matches()) throw new IllegalArgumentException("Invalid byte size format: " + sizeStr);
  
      double value = Double.parseDouble(matcher.group(1));
      String unit = matcher.group(3).toUpperCase(Locale.ROOT);
  
      switch (unit) {
        case "B":
          return (long) value;
        case "KB":
          return (long) (value * 1024);
        case "MB":
          return (long) (value * 1024 * 1024);
        case "GB":
          return (long) (value * 1024 * 1024 * 1024);
        case "TB":
          return (long) (value * 1024L * 1024L * 1024L * 1024L);
        default:
          throw new IllegalArgumentException("Unknown size unit: " + unit);
      }
    }
    public static long parseDuration(String durationStr) {
      if (durationStr == null) throw new IllegalArgumentException("Duration string cannot be null");
      Matcher matcher = DURATION_PATTERN.matcher(durationStr.trim());
      if (!matcher.matches()) throw new IllegalArgumentException("Invalid duration format: " + durationStr);
  
      double value = Double.parseDouble(matcher.group(1));
      String unit = matcher.group(3).toLowerCase(Locale.ROOT);
  
      switch (unit) {
        case "ns":
          return (long) value;
        case "ms":
          return (long) (value * 1_000_000);
        case "s":
        case "sec":
        case "seconds":
          return (long) (value * 1_000_000_000);
        case "m":
        case "min":
        case "minutes":
          return (long) (value * 60 * 1_000_000_000L);
        case "h":
        case "hr":
        case "hours":
          return (long) (value * 3600 * 1_000_000_000L);
        default:
          throw new IllegalArgumentException("Unknown time unit: " + unit);
      }
    }
  
    public static String formatByteSize(long bytes, String targetUnit) {
      targetUnit = targetUnit.toUpperCase(Locale.ROOT);
      switch (targetUnit) {
        case "B":
          return bytes + " B";
        case "KB":
          return String.format("%.2f KB", bytes / 1024.0);
        case "MB":
          return String.format("%.2f MB", bytes / (1024.0 * 1024));
        case "GB":
          return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        case "TB":
          return String.format("%.2f TB", bytes / (1024.0 * 1024 * 1024 * 1024));
        default:
          throw new IllegalArgumentException("Unknown byte unit: " + targetUnit);
      }
    }
  
    public static String formatDuration(long nanos, String targetUnit) {
      targetUnit = targetUnit.toLowerCase(Locale.ROOT);
      switch (targetUnit) {
        case "ns":
          return nanos + " ns";
        case "ms":
          return String.format("%.2f ms", nanos / 1_000_000.0);
        case "s":
        case "sec":
        case "seconds":
          return String.format("%.2f sec", nanos / 1_000_000_000.0);
        case "min":
        case "minutes":
          return String.format("%.2f min", nanos / (60.0 * 1_000_000_000));
        case "h":
        case "hr":
        case "hours":
          return String.format("%.2f hr", nanos / (3600.0 * 1_000_000_000));
        default:
          throw new IllegalArgumentException("Unknown duration unit: " + targetUnit);
      }
    }
  }