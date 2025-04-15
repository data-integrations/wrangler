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
  * A {@link Token} that represents a time duration value with units.
  */
 public class TimeDuration implements Token {
   private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)([Nn][Ss]|[Mm][Ss]|[Ss])");
   private final long nanoseconds;
   private final String originalValue;
 
   public TimeDuration(String value) {
     this.originalValue = value;
     Matcher matcher = PATTERN.matcher(value);
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid time duration format: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = matcher.group(2).toUpperCase();
 
     switch (unit) {
       case "NS":
         nanoseconds = (long) number;
         break;
       case "MS":
         nanoseconds = (long) (number * 1_000_000);
         break;
       case "S":
         nanoseconds = (long) (number * 1_000_000_000);
         break;
       default:
         throw new IllegalArgumentException("Unsupported time duration unit: " + unit);
     }
   }
 
   @Override
   public Object value() {
     return String.format("%.2f%s", getSeconds(), "s");
   }
 
   @Override
   public TokenType type() {
     return TokenType.TIME_DURATION;
   }
 
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", type().name());
     object.addProperty("value", originalValue);
     object.addProperty("nanoseconds", nanoseconds);
     return object;
   }
 
   public long getNanoseconds() {
     return nanoseconds;
   }
 
   public double getMilliseconds() {
     return nanoseconds / 1_000_000.0;
   }
 
   public double getSeconds() {
     return nanoseconds / 1_000_000_000.0;
   }
 } 