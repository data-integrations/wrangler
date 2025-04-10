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

 import com.google.gson.JsonObject;
 
 public class TimeDuration implements Token {
     private final String value;
     private final long nanoseconds;
 
     public TimeDuration(String value) {
         this.value = value;
         this.nanoseconds = parseNanoseconds(value);
     }
 
     public long getNanoseconds() {
         return nanoseconds;
     }
 
     @Override
     public Object value() {
         return value;
     }
 
     @Override
     public TokenType type() {
         return TokenType.TIME_DURATION;
     }
 
     @Override
     public JsonObject toJson() {
         JsonObject jsonObject = new JsonObject();
         jsonObject.addProperty("type", type().name());
         jsonObject.addProperty("value", value);
         jsonObject.addProperty("nanoseconds", nanoseconds);
         return jsonObject;
     }
 
     private long parseNanoseconds(String value) {
         // Extract the numeric part and the unit part
         long multiplier = 1L;
         char unit = value.charAt(value.length() - 1);
         switch (unit) {
             case 'm': multiplier = 1000L * 1000; break; // milliseconds
             case 's': multiplier = 1000L * 1000 * 1000; break; // seconds
             case 'h': multiplier = 1000L * 1000 * 1000 * 60 * 60; break; // hours
             case 'd': multiplier = 1000L * 1000 * 1000 * 60 * 60 * 24; break; // days
             default:
                 throw new IllegalArgumentException("Invalid time duration unit: " + unit);
         }
         // Extract the numeric part
         String numericPart = value.substring(0, value.length() - 1);
         return Long.parseLong(numericPart) * multiplier;
     }
 }