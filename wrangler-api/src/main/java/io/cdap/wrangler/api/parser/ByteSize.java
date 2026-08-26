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
 * Parses and validates human-readable byte size values (e.g., "10MB", "512KB").
 */
 public class ByteSize implements Token {
     private final long bytes;
 
     public ByteSize(String input) {
         this.bytes = parseByteSize(input);
     }
 
     private long parseByteSize(String input) {
         Pattern pattern = Pattern.compile("(?i)([0-9.]+)(B|KB|MB|GB|TB)");
         Matcher matcher = pattern.matcher(input.trim());
         if (!matcher.matches()) {
             throw new IllegalArgumentException("Invalid byte size format: " + input);
         }
 
         double value = Double.parseDouble(matcher.group(1));
         String unit = matcher.group(2).toUpperCase();
 
         switch (unit) {
             case "B": return (long) value;
             case "KB": return (long) (value * 1024);
             case "MB": return (long) (value * 1024 * 1024);
             case "GB": return (long) (value * 1024 * 1024 * 1024);
             case "TB": return (long) (value * 1024L * 1024 * 1024 * 1024);
             default: throw new IllegalArgumentException("Unknown unit: " + unit);
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
         return TokenType.BYTESIZE;
     }
 
     @Override
     public JsonElement toJson() {
         return new JsonPrimitive(bytes);
     }
 
     @Override
     public String toString() {
         return bytes + " bytes";
     }
 }

 
 
 

