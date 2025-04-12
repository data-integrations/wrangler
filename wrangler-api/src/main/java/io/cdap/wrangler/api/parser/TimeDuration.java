public class TimeDuration extends Token {
    private final double valueInMillis;
  
    public TimeDuration(String value) {
      super(value);
      this.valueInMillis = parse(value);
    }
  
    private double parse(String input) {
      input = input.trim().toLowerCase();
      double num = Double.parseDouble(input.replaceAll("[^0-9.]", ""));
      if (input.endsWith("ms")) return num;
      if (input.endsWith("s")) return num * 1000;
      if (input.endsWith("m")) return num * 60 * 1000;
      if (input.endsWith("h")) return num * 60 * 60 * 1000;
      throw new IllegalArgumentException("Unsupported Time Unit: " + input);
    }
  
    public double getMillis() {
      return valueInMillis;
    }
  }
  
