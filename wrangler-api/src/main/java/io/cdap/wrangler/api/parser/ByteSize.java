/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
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
 
 /**
  * Represents a byte size token (e.g. "10MB", "2GB", etc.)
  */
 public class ByteSize implements Token {
    private final String value;
    private final long sizeInBytes;
 
   /**
    * Constructs a ByteSize token.
    *
    * @param value        original string representation (e.g. "10MB")
    * @param sizeInBytes  parsed size in bytes
    */

    public static long getBytes(String size) {
      // Implement the logic to parse and return the size in bytes.
      // For example, parsing '10MB' to its byte equivalent.
      // Add the logic based on your implementation, e.g., using a regular expression.
      // Here, let's assume it's just a dummy example for parsing sizes.
      long bytes = 0;
      if (size.endsWith("MB")) {
        bytes = Long.parseLong(size.replace("MB", "")) * 1024 * 1024;
      }
      return bytes;
    }
    public ByteSize(String value) {
        this.value = value;
        this.sizeInBytes = parseSize(value); // add this method
      }

      private long parseSize(String input) {
        input = input.toUpperCase();
        if (input.endsWith("KB")) {
          return Long.parseLong(input.replace("KB", "")) * 1024;
        } else if (input.endsWith("MB")) {
          return Long.parseLong(input.replace("MB", "")) * 1024 * 1024;
        } else if (input.endsWith("GB")) {
          return Long.parseLong(input.replace("GB", "")) * 1024 * 1024 * 1024;
        } else if (input.endsWith("B")) {
          return Long.parseLong(input.replace("B", ""));
        }
        throw new IllegalArgumentException("Unsupported size: " + input);
      }

   
 
   /**
    * Returns the original string representation of the token.
    */
   @Override
   public String value() {
     return value;
   }
 
   /**
    * Returns the size in bytes as a long.
    */
   public long getSizeInBytes() {
     return sizeInBytes;
   }
 
   /**
    * Returns the token type as TokenType.BYTESIZE.
    */
   @Override
   public TokenType type() {
     return TokenType.BYTESIZE;
   }
 
   /**
    * Returns a JSON representation of the token.
    */
   @Override
   public JsonElement toJson() {
     return new JsonPrimitive(sizeInBytes);
   }
 }
 