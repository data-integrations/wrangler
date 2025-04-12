
 package io.cdap.wrangler.api.parser;

 import com.google.gson.JsonElement;
 import com.google.gson.JsonObject;
 import io.cdap.wrangler.api.annotations.PublicEvolving;
 
 import java.io.Serializable;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 
 /**
  * Represents a time duration value with support for different units (ns, ms, s, m, h, d).
  */
 @PublicEvolving
 public class TimeDuration implements Token, Serializable {
   private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*([nmshd])s?");
   private static final long[] MULTIPLIERS = {
     1L,                    // ns
     1000L,                 // ms
     1000L * 1000L,         // s
     1000L * 1000L * 60L,   // m
     1000L * 1000L * 60L * 60L, // h
     1000L * 1000L * 60L * 60L * 24L  // d
   };
 
   private final long nanoseconds;
   private final String original;
 
   /**
    * Creates a TimeDuration instance from a string representation.
    * @param value The string representation of the time duration (e.g., "100ms", "2.5s")
    * @throws IllegalArgumentException if the string cannot be parsed
    */
   public TimeDuration(String value) {
     this.original = value;
     Matcher matcher = PATTERN.matcher(value);
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid time duration format: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = matcher.group(2);
     int unitIndex = getUnitIndex(unit);
     this.nanoseconds = (long) (number * MULTIPLIERS[unitIndex]);
   }
 
   private int getUnitIndex(String unit) {
     switch (unit) {
       case "n": return 0;
       case "m": return 1;
       case "s": return 2;
       case "h": return 3;
       case "d": return 4;
       default:
         throw new IllegalArgumentException("Invalid unit: " + unit);
     }
   }
 
   /**
    * Returns the duration in nanoseconds.
    * @return The duration in nanoseconds
    */
   public long getNanoseconds() {
     return nanoseconds;
   }
 
   @Override
   public Object value() {
     return nanoseconds;
   }
 
   @Override
   public TokenType type() {
     return TokenType.TIME_DURATION;
   }
 
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("nanoseconds", nanoseconds);
     object.addProperty("original", original);
     return object;
   }
 
   @Override
   public String toString() {
     return original;
   }
 }