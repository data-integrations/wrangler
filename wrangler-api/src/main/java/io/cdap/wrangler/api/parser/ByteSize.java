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

 import com.google.gson.JsonElement;
 import com.google.gson.JsonObject;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 
 /**
  * A {@link Token} that represents a byte size value with units.
  */
 public class ByteSize implements Token {
   private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)([KkMmGgTtPp][Bb])");
   private final long bytes;
   private final String originalValue;
 
   public ByteSize(String value) {
     this.originalValue = value;
     Matcher matcher = PATTERN.matcher(value);
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid byte size format: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = matcher.group(2).toUpperCase();
 
     switch (unit) {
       case "KB":
         bytes = (long) (number * 1024);
         break;
       case "MB":
         bytes = (long) (number * 1024 * 1024);
         break;
       case "GB":
         bytes = (long) (number * 1024 * 1024 * 1024);
         break;
       case "TB":
         bytes = (long) (number * 1024L * 1024 * 1024 * 1024);
         break;
       case "PB":
         bytes = (long) (number * 1024L * 1024 * 1024 * 1024 * 1024);
         break;
       default:
         throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
     }
   }
 
   @Override
   public Object value() {
     return String.format("%.2f%s", getMB(), "MB");
   }
 
   @Override
   public TokenType type() {
     return TokenType.BYTE_SIZE;
   }
 
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", type().name());
     object.addProperty("value", originalValue);
     object.addProperty("bytes", bytes);
     return object;
   }
 
   public long getBytes() {
     return bytes;
   }
 
   public double getKB() {
     return bytes / 1024.0;
   }
 
   public double getMB() {
     return bytes / (1024.0 * 1024);
   }
 
   public double getGB() {
     return bytes / (1024.0 * 1024 * 1024);
   }
 
   public double getTB() {
     return bytes / (1024.0 * 1024 * 1024 * 1024);
   }
 
   public double getPB() {
     return bytes / (1024.0 * 1024 * 1024 * 1024 * 1024);
   }
 } 