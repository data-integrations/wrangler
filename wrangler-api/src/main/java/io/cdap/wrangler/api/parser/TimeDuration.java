/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

 package io.cdap.wrangler.api.parser;

 import com.google.gson.JsonElement;
 import com.google.gson.JsonPrimitive;
 
 /**
  * Represents a time duration with support for unit conversion.
  */
 public class TimeDuration {
     private final long nanoseconds;
     private final String originalValue;
 
     public TimeDuration(String value) {
         this.originalValue = value;
         this.nanoseconds = parseDuration(value);
     }
 
     private long parseDuration(String value) {
         String unit = value.replaceAll("[0-9.]", "").trim().toLowerCase();
         double num = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
         
         switch (unit) {
             case "ns": return (long) num;
             case "ms": return (long) (num * 1_000_000);
             case "s": return (long) (num * 1_000_000_000);
             case "m": return (long) (num * 60_000_000_000L);
             case "h": return (long) (num * 3_600_000_000_000L);
             default: throw new IllegalArgumentException("Unknown time unit: " + unit);
         }
     }
 
     public JsonElement toJson() {
         return new JsonPrimitive(originalValue);
     }
 
     public long getNanoseconds() {
         return nanoseconds;
     }
 
     public long getMilliseconds() {
         return nanoseconds / 1_000_000;
     }
 
     public long getSeconds() {
         return nanoseconds / 1_000_000_000;
     }
 
     public String getOriginalValue() {
         return originalValue;
     }
 
     @Override
     public String toString() {
         return originalValue;
     }
 }