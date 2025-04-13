private long parseSize(String sizeString) {
  if (sizeString == null || sizeString.trim().isEmpty()) {
      throw new IllegalArgumentException("Size string must not be null or empty.");
  }

  sizeString = sizeString.trim().toUpperCase();
  String numericPart;
  double multiplier;

  try {
      if (sizeString.endsWith("KB")) {
          numericPart = sizeString.substring(0, sizeString.length() - 2);
          multiplier = KILOBYTE;
      } else if (sizeString.endsWith("MB")) {
          numericPart = sizeString.substring(0, sizeString.length() - 2);
          multiplier = MEGABYTE;
      } else if (sizeString.endsWith("GB")) {
          numericPart = sizeString.substring(0, sizeString.length() - 2);
          multiplier = GIGABYTE;
      } else if (sizeString.endsWith("TB")) { // Added Terabyte
          numericPart = sizeString.substring(0, sizeString.length() - 2);
          multiplier = TERABYTE;
      } else if (sizeString.endsWith("B")) {
          numericPart = sizeString.substring(0, sizeString.length() - 1);
          multiplier = 1.0;
      } else {
          // Match the test's expected generic message format
          throw new IllegalArgumentException(
              "Invalid byte size format or unsupported unit in string: " + sizeString
          );
      }

      if (numericPart.isEmpty()) {
          throw new IllegalArgumentException("Missing numeric value in size string: " + sizeString);
      }

      double parsedValue = Double.parseDouble(numericPart);
      if (parsedValue < 0) {
          throw new IllegalArgumentException("Size value cannot be negative: " + sizeString);
      }
      // Cast to long truncates fractional bytes, which is reasonable for byte sizes.
      return (long) (parsedValue * multiplier);

  } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid numeric value in size string: " + sizeString, e);
  }
}

/**
* Returns the size in bytes.
*
* @return The size in bytes.
*/
public long getBytes() {
  return value;
}

/**
* Returns the size in kilobytes (double for potential fractions).
*
* @return The size in kilobytes.
*/
public double getKiloBytes() {
  return value / KILOBYTE;
}

/**
* Returns the size in megabytes (double for potential fractions).
*
* @return The size in megabytes.
*/
public double getMegaBytes() {
  return value / MEGABYTE;
}

/**
* Returns the size in gigabytes (double for potential fractions).
*
* @return The size in gigabytes.
*/
public double getGigaBytes() {
  return value / GIGABYTE;
}

/**
* Returns the size in terabytes (double for potential fractions).
*
* @return The size in terabytes.
*/
public double getTeraBytes() {
  return value / TERABYTE;
}

@Override
public Object value() {
  return value;
}

@Override
public TokenType type() {
  return TokenType.BYTE_SIZE;
}

@Override
public JsonElement toJson() {
  JsonObject object = new JsonObject();
  object.addProperty("type", TokenType.BYTE_SIZE.name());
  object.addProperty("value", value); // Store the canonical long value
  return object;
}

@Override
public String toString() {
  // Provide a reasonable string representation, maybe the original input if stored,
  // or reconstruct from the byte value. For simplicity, just return bytes.
  return value + "B";
}
}