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
 import com.google.gson.JsonPrimitive;
 import io.cdap.wrangler.api.annotations.PublicEvolving;
 
 /**
  * The TimeDuration class wraps a time duration value (e.g., "100ms", "2s") in an object.
  * An object of type {@code TimeDuration} contains the value as a {@code long} representing
  * the duration in nanoseconds. Along with the time duration value, this object also contains
  * the value that represents the type of this object as {@code TokenType}.
  *
  * <p>This class provides methods to extract the value held by this wrapper object
  * in nanoseconds and the type of the token.</p>
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
  * @see ByteSize
  */
 @PublicEvolving
 public class TimeDuration implements Token {
   /**
    * The {@code long} value representing the duration in nanoseconds.
    */
   private final long nanos;
 
   /**
    * Allocates a {@code TimeDuration} object by parsing the input string (e.g., "100ms").
    *
    * @param value the string representing the time duration (e.g., "100ms", "2s")
    * @throws IllegalArgumentException if the input string is invalid or contains an unknown unit
    */
   public TimeDuration(String value) {
     if (value == null || value.trim().isEmpty()) {
       throw new IllegalArgumentException("Time duration value cannot be null or empty");
     }
 
     // Extract numeric part and unit
     String unit = value.replaceAll("[0-9.]", "").toUpperCase();
     String numStr = value.replaceAll("[^0-9.]", "");
 
     if (numStr.isEmpty() || unit.isEmpty()) {
       throw new IllegalArgumentException("Invalid time duration format: " + value);
     }
 
     try {
       double num = Double.parseDouble(numStr);
       switch (unit) {
         case "NS":
           nanos = (long) num;
           break;
         case "MS":
           nanos = (long) (num * 1_000_000);
           break;
         case "S":
           nanos = (long) (num * 1_000_000_000);
           break;
         case "M":
           nanos = (long) (num * 60 * 1_000_000_000);
           break;
         case "H":
           nanos = (long) (num * 3600 * 1_000_000_000);
           break;
         default:
           throw new IllegalArgumentException("Unknown time duration unit: " + unit);
       }
     } catch (NumberFormatException e) {
       throw new IllegalArgumentException("Invalid numeric value in time duration: " + numStr, e);
     }
   }
 
   /**
    * Returns the value of this {@code TimeDuration} object as a long representing nanoseconds.
    *
    * @return the primitive {@code long} value of this object in nanoseconds
    */
   @Override
   public Long value() {
     return nanos;
   }
 
   /**
    * Returns the type of this {@code TimeDuration} object as a {@code TokenType} enum.
    *
    * @return the enumerated {@code TokenType} of this object
    */
   @Override
   public TokenType type() {
     return TokenType.TIME_DURATION;
   }
 
   /**
    * Returns the value of this {@code TimeDuration} object as a {@code JsonElement}.
    *
    * @return JSON representation of this {@code TimeDuration} object as {@code JsonElement}
    */
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", TokenType.TIME_DURATION.name());
     object.add("value", new JsonPrimitive(nanos));
     return object;
   }
 }