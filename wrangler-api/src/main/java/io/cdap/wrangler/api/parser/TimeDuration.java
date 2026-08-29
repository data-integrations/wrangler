/*
 * Copyright © 2024 Cask Data, Inc.
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
 import io.cdap.wrangler.api.annotations.PublicEvolving;
 
 /**
  * Represents a time duration value with units (ms, s, m, h, d).
  */
 @PublicEvolving
 public class TimeDuration implements Token {
   private final String value;
   private final long milliseconds;
 
   public TimeDuration(String value) {
     this.value = value;
     this.milliseconds = parseMilliseconds(value);
   }
 
   private static long parseMilliseconds(String value) {
     String number = value.replaceAll("[^0-9.]", "");
     String unit = value.replaceAll("[0-9.]", "").toLowerCase();
     double duration = Double.parseDouble(number);
     
     switch (unit) {
      case "ms":
         return (long) duration;
      case "s":
         return (long) (duration * 1000);
      case "m":
      case "min":
         return (long) (duration * 60 * 1000);
      case "h":
         return (long) (duration * 60 * 60 * 1000);
      case "d":
         return (long) (duration * 24 * 60 * 60 * 1000);
      case "us":
         return (long) (duration / 1000.0); // Convert microseconds to milliseconds
      case "ns":
         return (long) (duration / 1000000.0); // Convert nanoseconds to milliseconds
      default:
         return (long) duration; // Default case
}

   }
 
   @Override
   public String value() {
     return value;
   }
 
   public long getMilliseconds() {
     return milliseconds;
   }
 
   @Override
   public TokenType type() {
     return TokenType.TIME_DURATION;
   }
 
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", TokenType.TIME_DURATION.name());
     object.addProperty("value", value);
     object.addProperty("milliseconds", milliseconds);
     return object;
   }
 }
