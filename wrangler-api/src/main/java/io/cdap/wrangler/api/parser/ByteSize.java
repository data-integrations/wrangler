/*
 * Copyright © 2017-2029 Cask Data, Inc.
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
 import com.google.gson.JsonPrimitive;
 import io.cdap.wrangler.api.annotations.PublicEvolving;
 
 /**
  * The ByteSize class wraps a byte size value (e.g., "10KB", "1.5MB") in an object.
  * An object of type {@code ByteSize} contains the value as a {@code long} representing
  * the size in bytes. Along with the byte size value, this object also contains
  * the value that represents the type of this object as {@code TokenType}.
  *
  * <p>This class provides methods to extract the value held by this wrapper object
  * in bytes and the type of the token.</p>
  *
  * @see Bool
  * @see ColumnName
  * @see ColumnNameList
  * @see DirectiveName
  * @see Numeric
  * @see NumericList
  * @see Properties
  * @see Ranges
  * @see Expression
  * @see Text
  * @see TextList
  * @see TimeDuration
  */
 @PublicEvolving
 public class ByteSize implements Token {
   /**
    * The {@code long} value representing the size in bytes.
    */
   private final long bytes;
 
   /**
    * Allocates a {@code ByteSize} object by parsing the input string (e.g., "10KB").
    *
    * @param value the string representing the byte size (e.g., "10KB", "1.5MB")
    * @throws IllegalArgumentException if the input string is invalid or contains an unknown unit
    */
   public ByteSize(String value) {
     if (value == null || value.trim().isEmpty()) {
       throw new IllegalArgumentException("Byte size value cannot be null or empty");
     }
 
     // Extract numeric part and unit
     String unit = value.replaceAll("[0-9.]", "").toUpperCase();
     String numStr = value.replaceAll("[^0-9.]", "");
 
     if (numStr.isEmpty() || unit.isEmpty()) {
       throw new IllegalArgumentException("Invalid byte size format: " + value);
     }
 
     try {
       double num = Double.parseDouble(numStr);
       switch (unit) {
         case "B":
           bytes = (long) num;
           break;
         case "KB":
           bytes = (long) (num * 1024);
           break;
         case "MB":
           bytes = (long) (num * 1024 * 1024);
           break;
         case "GB":
           bytes = (long) (num * 1024 * 1024 * 1024);
           break;
         case "TB":
           bytes = (long) (num * 1024 * 1024 * 1024 * 1024);
           break;
         default:
           throw new IllegalArgumentException("Unknown byte size unit: " + unit);
       }
     } catch (NumberFormatException e) {
       throw new IllegalArgumentException("Invalid numeric value in byte size: " + numStr, e);
     }
   }
 
   /**
    * Returns the value of this {@code ByteSize} object as a long representing bytes.
    *
    * @return the primitive {@code long} value of this object in bytes
    */
   @Override
   public Long value() {
     return bytes;
   }
 
   /**
    * Returns the type of this {@code ByteSize} object as a {@code TokenType} enum.
    *
    * @return the enumerated {@code TokenType} of this object
    */
   @Override
   public TokenType type() {
     return TokenType.BYTE_SIZE;
   }
 
   /**
    * Returns the value of this {@code ByteSize} object as a {@code JsonElement}.
    *
    * @return JSON representation of this {@code ByteSize} object as {@code JsonElement}
    */
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", TokenType.BYTE_SIZE.name());
     object.add("value", new JsonPrimitive(bytes));
     return object;
   }
 }