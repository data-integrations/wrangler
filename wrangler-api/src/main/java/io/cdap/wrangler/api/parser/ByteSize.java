package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;


@PublicEvolving
public class ByteSize implements Token {
  private static final Pattern BYTE_PATTERN = Pattern.compile("([0-9]+(\\.[0-9]+)?)([A-Za-z]+)");
  private final long bytes;

  public ByteSize(String value) {
    super();
    Matcher matcher = BYTE_PATTERN.matcher(value);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid byte size format: " + value);
    }

    double size = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(3).toUpperCase();

    switch (unit) {
      case "B": bytes = (long) size; break;
      case "KB": bytes = (long) (size * 1000); break;
      case "MB": bytes = (long) (size * 1000 * 1000); break;
      case "GB": bytes = (long) (size * 1000 * 1000 * 1000); break;
      case "TB": bytes = (long) (size * 1000 * 1000 * 1000 * 1000); break;
      case "KIB": bytes = (long) (size * 1024); break;
      case "MIB": bytes = (long) (size * 1024 * 1024); break;
      case "GIB": bytes = (long) (size * 1024 * 1024 * 1024); break;
      case "TIB": bytes = (long) (size * 1024 * 1024 * 1024 * 1024); break;
      default: throw new IllegalArgumentException("Unknown byte unit: " + unit);
    }
  }

  public long getBytes() {
    return bytes;
  }

  @Override
  public Object value() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'value'");
  }

  @Override
  public TokenType type() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'type'");
  }

  @Override
  public JsonElement toJson() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'toJson'");
  }
}