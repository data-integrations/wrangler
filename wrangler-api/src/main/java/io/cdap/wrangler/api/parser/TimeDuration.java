/*
 * Copyright © 2025 Garv Tayal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.cdap.wrangler.api.parser;

 import com.google.gson.JsonElement;
 import com.google.gson.JsonPrimitive;
 
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 
 /**
 * Parses and validates human-readable time duration values (e.g., "5s", "2h").
 */

 public class TimeDuration implements Token {
     private final long milliseconds;
 
     public TimeDuration(String input) {
         this.milliseconds = parseDuration(input);
     }
 
     private long parseDuration(String input) {
         Pattern pattern = Pattern.compile("(?i)([0-9.]+)(ms|s|sec|m|min|h|hr)");
         Matcher matcher = pattern.matcher(input.trim());
         if (!matcher.matches()) {
             throw new IllegalArgumentException("Invalid time duration format: " + input);
         }
 
         double value = Double.parseDouble(matcher.group(1));
         String unit = matcher.group(2).toLowerCase();
 
         switch (unit) {
             case "ms": return (long) value;
             case "s":
             case "sec": return (long) (value * 1000);
             case "m":
             case "min": return (long) (value * 60 * 1000);
             case "h":
             case "hr": return (long) (value * 60 * 60 * 1000);
             default: throw new IllegalArgumentException("Unknown unit: " + unit);
         }
     }
 
     public long getMilliseconds() {
         return milliseconds;
     }
 
     @Override
     public Object value() {
         return milliseconds;
     }
 
     @Override
     public TokenType type() {
         return TokenType.DURATION;
     }
 
     @Override
     public JsonElement toJson() {
         return new JsonPrimitive(milliseconds);
     }
 
     @Override
     public String toString() {
         return milliseconds + " ms";
     }
 }
 

