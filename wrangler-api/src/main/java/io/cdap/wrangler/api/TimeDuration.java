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

 package io.cdap.wrangler.api;

 import com.google.gson.JsonElement;
 import com.google.gson.JsonPrimitive;
 import io.cdap.wrangler.api.parser.Token;
 import io.cdap.wrangler.api.parser.TokenType;
 
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 
 /**
  * Represents a time duration token (e.g., "5s", "1m").
  */
 public class TimeDuration implements Token {
     private static final Pattern TIME_PATTERN = Pattern.compile("(\\d+)(ms|s|m|h|d)", Pattern.CASE_INSENSITIVE);
     private final long millis;
 
     public TimeDuration(String token) {
         Matcher matcher = TIME_PATTERN.matcher(token.trim());
         if (!matcher.matches()) {
             throw new IllegalArgumentException("Invalid time duration: " + token);
         }
 
         long value = Long.parseLong(matcher.group(1));
         String unit = matcher.group(2).toLowerCase();
 
         switch (unit) {
             case "ms":
                 this.millis = value;
                 break;
             case "s":
                 this.millis = value * 1000;
                 break;
             case "m":
                 this.millis = value * 60 * 1000;
                 break;
             case "h":
                 this.millis = value * 60 * 60 * 1000;
                 break;
             case "d":
                 this.millis = value * 24 * 60 * 60 * 1000;
                 break;
             default:
                 throw new IllegalArgumentException("Unsupported time unit: " + unit);
         }
     }
 
     public long getMillis() {
         return millis;
     }
 
     @Override
     public Object value() {
         return millis;
     }
 
     @Override
     public TokenType type() {
         return TokenType.TIME_DURATION;
     }
 
     @Override
     public JsonElement toJson() {
         return new JsonPrimitive(millis);
     }
 }
 