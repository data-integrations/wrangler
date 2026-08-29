

 package io.cdap.wrangler.api.parser;

 import com.google.gson.JsonElement;
 import com.google.gson.JsonObject;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 
 
 public class TimeDuration implements Token {
   private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)([Nn][Ss]|[Mm][Ss]|[Ss])");
   private final long nanoseconds;
   private final String originalValue;
 
   public TimeDuration(String value) {
     this.originalValue = value;
     Matcher matcher = PATTERN.matcher(value);
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid time duration format: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = matcher.group(2).toUpperCase();
 
     switch (unit) {
       case "NS":
         nanoseconds = (long) number;
         break;
       case "MS":
         nanoseconds = (long) (number * 1_000_000);
         break;
       case "S":
         nanoseconds = (long) (number * 1_000_000_000);
         break;
       default:
         throw new IllegalArgumentException("Unsupported time duration unit: " + unit);
     }
   }
 
   @Override
   public Object value() {
     return String.format("%.2f%s", getSeconds(), "s");
   }
 
   @Override
   public TokenType type() {
     return TokenType.TIME_DURATION;
   }
 
   @Override
   public JsonElement toJson() {
     JsonObject object = new JsonObject();
     object.addProperty("type", type().name());
     object.addProperty("value", originalValue);
     object.addProperty("nanoseconds", nanoseconds);
     return object;
   }
 
   public long getNanoseconds() {
     return nanoseconds;
   }
 
   public double getMilliseconds() {
     return nanoseconds / 1_000_000.0;
   }
 
   public double getSeconds() {
     return nanoseconds / 1_000_000_000.0;
   }
 } 
