

 package io.cdap.wrangler.api.parser;

 import com.google.gson.JsonElement;
 import com.google.gson.JsonObject;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 

 public class ByteSize implements Token {
   private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)([KkMmGgTtPp][Bb])");
   private final long bytes;
   private final String originalValue;
 
   public ByteSize(String value) {
     this.originalValue = value;
     Matcher matcher = PATTERN.matcher(value);
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid byte size format: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = matcher.group(2).toUpperCase();
 
     switch (unit) {
       case "KB":
         bytes = (long) (number * 1024);
         break;
       case "MB":
         bytes = (long) (number * 1024 * 1024);
         break;
       case "GB":
         bytes = (long) (number * 1024 * 1024 * 1024);
         break;
       case "TB":
         bytes = (long) (number * 1024L * 1024 * 1024 * 1024);
         break;
       case "PB":
         bytes = (long) (number * 1024L * 1024 * 1024 * 1024 * 1024);
         break;
       default:
         throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
     }
   }
 
   @Override
   public Object value() {
     return String.format("%.2f%s", getMB(), "MB");
   }
 
   @Override
   public TokenType type() {
     return TokenType.BYTE_SIZE;
   }
 
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", type().name());
     object.addProperty("value", originalValue);
     object.addProperty("bytes", bytes);
     return object;
   }
 
   public long getBytes() {
     return bytes;
   }
 
   public double getKB() {
     return bytes / 1024.0;
   }
 
   public double getMB() {
     return bytes / (1024.0 * 1024);
   }
 
   public double getGB() {
     return bytes / (1024.0 * 1024 * 1024);
   }
 
   public double getTB() {
     return bytes / (1024.0 * 1024 * 1024 * 1024);
   }
 
   public double getPB() {
     return bytes / (1024.0 * 1024 * 1024 * 1024 * 1024);
   }
 } 
