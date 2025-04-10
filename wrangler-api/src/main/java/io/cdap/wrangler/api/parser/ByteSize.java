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
 
 public class ByteSize implements Token {
     private final String value;
     private final long bytes;
 
     public ByteSize(String value) {
         this.value = value;
         this.bytes = parseBytes(value);
     }
 
     public long getBytes() {
         return bytes;
     }
 
     @Override
     public Object value() {
         return value;
     }
 
     @Override
     public TokenType type() {
         return TokenType.BYTE_SIZE;
     }
 
     @Override
     public JsonObject toJson() {
         JsonObject jsonObject = new JsonObject();
         jsonObject.addProperty("type", type().name());
         jsonObject.addProperty("value", value);
         jsonObject.addProperty("bytes", bytes);
         return jsonObject;
     }
 
     private long parseBytes(String value) {
         // Extract the numeric part and the unit part
         long multiplier = 1L;
         char unit = value.charAt(value.length() - 1);
         if (Character.isDigit(unit)) {
             // No unit specified, assume bytes
             return Long.parseLong(value);
         }
         switch (unit) {
             case 'K': multiplier = 1024L; break;
             case 'M': multiplier = 1024L * 1024; break;
             case 'G': multiplier = 1024L * 1024 * 1024; break;
             case 'T': multiplier = 1024L * 1024 * 1024 * 1024; break;
             case 'P': multiplier = 1024L * 1024 * 1024 * 1024 * 1024; break;
             default:
                 throw new IllegalArgumentException("Invalid byte size unit: " + unit);
         }
         // Extract the numeric part
         String numericPart = value.substring(0, value.length() - 1);
         return Long.parseLong(numericPart) * multiplier;
     }
 }