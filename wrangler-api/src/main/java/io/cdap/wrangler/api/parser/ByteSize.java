public class ByteSize extends Token {
    private final double valueInBytes;
  
    public ByteSize(String value) {
      super(value);
      this.valueInBytes = parse(value);
    }
  
    private double parse(String input) {
      input = input.trim().toLowerCase();
      double num = Double.parseDouble(input.replaceAll("[^0-9.]", ""));
      if (input.endsWith("kb")) return num * 1024;
      if (input.endsWith("mb")) return num * 1024 * 1024;
      if (input.endsWith("gb")) return num * 1024 * 1024 * 1024;
      if (input.endsWith("b")) return num;
      throw new IllegalArgumentException("Unsupported Byte Size: " + input);
    }
  
    public double getBytes() {
      return valueInBytes;
    }
  }
  
