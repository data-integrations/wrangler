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

 import com.google.gson.JsonObject;
 
 import java.util.Objects;
 import java.util.concurrent.TimeUnit;
 
 /**
  * Represents a time duration token, e.g., "5s", "2m", "1h", etc.
  */
 public class TimeDuration implements Token {
   private final long value;
   private final String unit;
 
   /**
    * Constructs a TimeDuration token.
    *
    * @param value the numeric part of the duration (e.g., 5)
    * @param unit  the time unit (e.g., "s", "m", "h")
    */
   public TimeDuration(long value, String unit) {
     this.value = value;
     this.unit = unit;
   }
 
   /**
    * Returns the numeric value of the duration.
    */
    public static long getDuration(String duration, TimeUnit unit) {
    // For example, parsing '2h' to its equivalent in milliseconds.
    long durationInMillis = 0;
    if (duration.endsWith("h")) {
      durationInMillis = Long.parseLong(duration.replace("h", "")) * 60 * 60 * 1000;
    }
    return unit.convert(durationInMillis, TimeUnit.MILLISECONDS);
  }
   public long getValue() {
     return value;
   }
 
   /**
    * Returns the time unit (e.g., "s", "m", "h").
    */
   public String getUnit() {
     return unit;
   }
 
   /**
    * Returns the token type as TokenType.TIMEDURATION.
    */
   @Override
   public TokenType type() {
     return TokenType.TIMEDURATION;
   }
 
   /**
    * Returns the JSON representation of this token.
    */
   @Override
   public JsonObject toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", type().name());
     object.addProperty("value", value);
     object.addProperty("unit", unit);
     return object;
   }
 
   /**
    * Returns the original string representation (e.g., "5s", "2m").
    */
   @Override
   public String value() {
     return value + unit;
   }
 
   @Override
   public boolean equals(Object o) {
     if (this == o){
        return true;
     } 
     if (!(o instanceof TimeDuration)){
        return false;
     }
     TimeDuration that = (TimeDuration) o;
     return value == that.value && Objects.equals(unit, that.unit);
   }
 
   @Override
   public int hashCode() {
     return Objects.hash(value, unit);
   }
 
   @Override
   public String toString() {
     return value + unit;
   }
 }
 