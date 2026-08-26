 package io.cdap.wrangler.api.parser;

 import com.google.gson.JsonElement;
 import com.google.gson.JsonObject;
 import io.cdap.wrangler.api.annotations.PublicEvolving;
 
 import java.io.Serializable;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 
 /**
  * Represents a byte size value with support for different units (B, KB, MB, GB, TB, PB).
  */
 @PublicEvolving
 public class ByteSize implements Token, Serializable {
   private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*([BKMGTP]B)");
   private static final long[] MULTIPLIERS = {
     1L,                    // B
     1024L,                 // KB
     1024L * 1024L,         // MB
     1024L * 1024L * 1024L, // GB
     1024L * 1024L * 1024L * 1024L, // TB
     1024L * 1024L * 1024L * 1024L * 1024L  // PB
   };
 
   private final long bytes;
   private final String original;
 
   /**
    * Creates a ByteSize instance from a string representation.
    * @param value The string representation of the byte size (e.g., "10KB", "1.5MB")
    * @throws IllegalArgumentException if the string cannot be parsed
    */
   public ByteSize(String value) {
     this.original = value;
     Matcher matcher = PATTERN.matcher(value);
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid byte size format: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = matcher.group(2);
     int unitIndex = getUnitIndex(unit);
     this.bytes = (long) (number * MULTIPLIERS[unitIndex]);
   }
 
   private int getUnitIndex(String unit) {
     switch (unit) {
       case "B": return 0;
       case "KB": return 1;
       case "MB": return 2;
       case "GB": return 3;
       case "TB": return 4;
       case "PB": return 5;
       default:
         throw new IllegalArgumentException("Invalid unit: " + unit);
     }
   }
 
   /**
    * Returns the size in bytes.
    * @return The size in bytes
    */
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
     JsonObject object = new JsonObject();
     object.addProperty("bytes", bytes);
     object.addProperty("original", original);
     return object;
   }
 
   @Override
   public String toString() {
     return original;
   }
 }