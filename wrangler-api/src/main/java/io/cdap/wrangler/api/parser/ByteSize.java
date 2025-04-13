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

 package io.cdap.wrangler.api.parser;

 import com.google.gson.JsonElement;
 import com.google.gson.JsonPrimitive;
 
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 
 /**
  * Token implementation to handle byte size values like "10KB", "5MB", etc.
  */
 public class ByteSize implements Token {
   private static final Pattern BYTE_PATTERN = Pattern.compile("(?i)(\\d+(?:\\.\\d+)?)(B|KB|MB|GB|TB|PB|EB)");
 
   private final String originalValue;
   private final double valueInBytes;
 
   public ByteSize(String value) {
     this.originalValue = value;
     this.valueInBytes = parse(value);
   }
 
   private double parse(String value) {
     Matcher matcher = BYTE_PATTERN.matcher(value.trim());
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid byte size value: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = matcher.group(2).toUpperCase();
 
     switch (unit) {
       case "B":
         return number;
       case "KB":
         return number * 1024L;
       case "MB":
         return number * 1024L * 1024L;
       case "GB":
         return number * 1024L * 1024L * 1024L;
       case "TB":
         return number * 1024L * 1024L * 1024L * 1024L;
       case "PB":
         return number * 1024L * 1024L * 1024L * 1024L * 1024L;
       case "EB":
         return number * 1024L * 1024L * 1024L * 1024L * 1024L * 1024L;
       default:
         throw new IllegalArgumentException("Unknown byte unit: " + unit);
     }
   }
 
   public double getBytes() {
     return valueInBytes;
   }
 
   @Override
   public Object value() {
     return valueInBytes;
   }
 
   @Override
   public TokenType type() {
     return TokenType.BYTE_SIZE;
   }
 
   @Override
   public JsonElement toJson() {
     return new JsonPrimitive(valueInBytes);
   }
 
   @Override
   public String toString() {
     return originalValue;
   }
 }