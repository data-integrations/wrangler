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
  * Represents a byte size value with units (KB, MB, GB, TB).
  */
 @PublicEvolving
 public class ByteSize implements Token {
   private final String value;
   private final long bytes;
 
   public ByteSize(String value) {
     this.value = value;
     this.bytes = parseBytes(value);
   }
 
   private static long parseBytes(String value) {
     String number = value.replaceAll("[^0-9.]", "");
     String unit = value.replaceAll("[0-9.]", "").toLowerCase();
     double size = Double.parseDouble(number);
     
     switch (unit) {
       case "kb":
         return (long) (size * 1024);
       case "mb":
         return (long) (size * 1024 * 1024); 
       case "gb":
         return (long) (size * 1024 * 1024 * 1024);
       case "tb":
         return (long) (size * 1024L * 1024L * 1024L * 1024L);
       default:
         return (long) size; // Base unit bytes
     }
   }
 
   @Override
   public String value() {
     return value;
   }
 
   public long getBytes() {
     return bytes;
   }
 
   @Override
   public TokenType type() {
     return TokenType.BYTE_SIZE;
   }
 
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", TokenType.BYTE_SIZE.name());
     object.addProperty("value", value);
     object.addProperty("bytes", bytes);
     return object;
   }
 }
