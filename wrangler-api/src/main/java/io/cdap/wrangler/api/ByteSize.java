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
  * Represents a byte size token (e.g., "10MB", "5KB").
  */
 public class ByteSize implements Token {
     private static final Pattern BYTE_PATTERN = Pattern.compile("(\\d+)([KMGTP]?B)", Pattern.CASE_INSENSITIVE);
     private final long bytes;
 
     public ByteSize(String token) {
         Matcher matcher = BYTE_PATTERN.matcher(token.trim());
         if (!matcher.matches()) {
             throw new IllegalArgumentException("Invalid byte size: " + token);
         }
 
         long value = Long.parseLong(matcher.group(1));
         String unit = matcher.group(2).toUpperCase();
 
         switch (unit) {
             case "KB":
                 this.bytes = value * 1024;
                 break;
             case "MB":
                 this.bytes = value * 1024 * 1024;
                 break;
             case "GB":
                 this.bytes = value * 1024 * 1024 * 1024;
                 break;
             case "TB":
                 this.bytes = value * 1024L * 1024 * 1024 * 1024;
                 break;
             case "PB":
                 this.bytes = value * 1024L * 1024 * 1024 * 1024 * 1024;
                 break;
             case "B":
             default:
                 this.bytes = value;
         }
     }
 
     public long getBytes() {
         return bytes;
     }
 
     @Override
     public Object value() {
         return bytes;
     }
 
     @Override
     public TokenType type() {
         return TokenType.BYTE_SIZE;
     }
 
     @Override
     public JsonElement toJson() {
         return new JsonPrimitive(bytes);
     }
 }
 