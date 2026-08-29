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
  * Represents a byte size value with support for unit conversion.
  */
 public class ByteSize {
     private final long bytes;
     private final String originalValue;
 
     public ByteSize(String value) {
         this.originalValue = value;
         this.bytes = parseBytes(value);
     }
 
     private long parseBytes(String value) {
         String unit = value.replaceAll("[0-9.]", "").trim().toUpperCase();
         double num = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
         
         switch (unit) {
             case "B": return (long) num;
             case "KB": return (long) (num * 1024);
             case "MB": return (long) (num * 1024 * 1024);
             case "GB": return (long) (num * 1024 * 1024 * 1024);
             case "TB": return (long) (num * 1024L * 1024 * 1024 * 1024);
             default: throw new IllegalArgumentException("Unknown byte unit: " + unit);
         }
     }
 
     public JsonElement toJson() {
         return new JsonPrimitive(originalValue);
     }
 
     public long getBytes() {
         return bytes;
     }
 
     public String getOriginalValue() {
         return originalValue;
     }
 
     @Override
     public String toString() {
         return originalValue;
     }
 }