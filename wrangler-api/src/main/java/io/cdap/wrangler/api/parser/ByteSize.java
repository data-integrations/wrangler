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
 import io.cdap.wrangler.api.annotations.PublicEvolving;
 
 /**
  * The ByteSize class wraps byte size values with units (e.g. "10KB", "1.5MB") in an object.
  * An object of type ByteSize contains the value in bytes as well as the type of the token this class represents.
  *
  * <p>This class provides methods to:
  * 1. Parse byte size strings with units into canonical bytes
  * 2. Convert between different byte size units
  * 3. Retrieve the value in bytes or in a specified unit
  * </p>
  */
  
 @PublicEvolving
 public class ByteSize implements Token {
   private final long bytes;
   private final String originalString;
 
   // Common byte size units and their multipliers (using binary prefixes)
   private static final long KB = 1024L;
   private static final long MB = KB * 1024L;
   private static final long GB = MB * 1024L;
   private static final long TB = GB * 1024L;
   private static final long PB = TB * 1024L;
 
   public ByteSize(String value) {
     if (value == null) {
       throw new IllegalArgumentException("Byte size value cannot be null");
     }
     this.originalString = value;
     this.bytes = parseByteSize(value);
   }
 
   private long parseByteSize(String value) {
     value = value.trim().toUpperCase();
     if (value.isEmpty()) {
       return 0L;
     }
 
     // Extract the numeric part and unit
     int unitIndex = 0;
     while (unitIndex < value.length() && 
            (Character.isDigit(value.charAt(unitIndex)) || 
             value.charAt(unitIndex) == '.' || 
             value.charAt(unitIndex) == '-')) {
       unitIndex++;
     }
 
     if (unitIndex == 0 || unitIndex == value.length()) {
       throw new IllegalArgumentException("Invalid byte size format: " + value);
     }
 
     try {
       double number = Double.parseDouble(value.substring(0, unitIndex));
       if (number < 0) {
         throw new IllegalArgumentException("Byte size cannot be negative: " + value);
       }
       
       String unit = value.substring(unitIndex).trim();
 
       // Convert to bytes based on unit
       switch (unit) {
         case "B":
           return (long) number;
         case "KB":
         case "K":
           return (long) (number * KB);
         case "MB":
         case "M":
           return (long) (number * MB);
         case "GB":
         case "G":
           return (long) (number * GB);
         case "TB":
         case "T":
           return (long) (number * TB);
         case "PB":
         case "P":
           return (long) (number * PB);
         default:
           throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
       }
     } catch (NumberFormatException e) {
       throw new IllegalArgumentException("Invalid numeric format in byte size: " + value, e);
     }
   }
 
   /**
    * Returns the value in bytes.
    *
    * @return the value in bytes
    */
   @Override
   public Long value() {
     return bytes;
   }
 
   /**
    * Returns the type of this ByteSize object as a TokenType enum.
    *
    * @return the enumerated TokenType of this object
    */
   @Override
   public TokenType type() {
     return TokenType.BYTE_SIZE;
   }
 
   /**
    * Returns the members of this ByteSize object as a JsonElement.
    *
    * @return Json representation of this ByteSize object as JsonElement
    */
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", TokenType.BYTE_SIZE.name());
     object.addProperty("value", originalString);
     object.addProperty("bytes", bytes);
     return object;
   }
 
   /**
    * Gets the value in a specified unit.
    *
    * @param unit the unit to convert to (B, KB, MB, GB, TB, PB)
    * @return the value in the specified unit
    * @throws IllegalArgumentException if an unsupported unit is specified
    */
   public double getValue(String unit) {
     unit = unit.toUpperCase();
     switch (unit) {
       case "B":
         return bytes;
       case "KB":
       case "K":
         return (double) bytes / KB;
       case "MB":
       case "M":
         return (double) bytes / MB;
       case "GB":
       case "G":
         return (double) bytes / GB;
       case "TB":
       case "T":
         return (double) bytes / TB;
       case "PB":
       case "P":
         return (double) bytes / PB;
       default:
         throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
     }
   }
 
   /**
    * Gets the original string representation of the byte size.
    *
    * @return the original string representation
    */
   public String getOriginalString() {
     return originalString;
   }
 }