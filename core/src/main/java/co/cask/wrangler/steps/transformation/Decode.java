package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.AbstractIndependentStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * A Wrangler step that decodes a column that was encoded as base-64,base-32 or hex.
 */
@Usage(
  directive = "decode",
  usage = "decode <base64|base32|hex> column",
  description = "Decodes a column"
)
public class Decode extends AbstractIndependentStep {
  private final Base64 base64Encode = new Base64();
  private final Base32 base32Encode = new Base32();
  private final Hex hexEncode = new Hex();
  private final Type type;
  private final String column;

  /**
   * Defines encoding types supported.
   */
  public enum Type {
    BASE64("BASE64"),
    BASE32("BASE32"),
    HEX("HEX");

    private String type;

    Type(String type) {
      this.type = type;
    }

    String getType() {
      return type;
    }
  }

  public Decode(int lineno, String directive, Type type, String column) {
    super(lineno, directive, column);
    this.type = type;
    this.column = column;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx == -1) {
        continue;
      }

      Object object = record.getValue(idx);
      if (object == null) {
        continue;
      }

      byte[] value = new byte[0];
      if (object instanceof String) {
        value = ((String) object).getBytes();
      } else if (object instanceof byte[]) {
        value = (byte[]) object;
      } else {
        throw new StepException(
          String.format("%s : Invalid value type '%s' of column '%s'. Should be of type string or byte array, "
            , toString(), value.getClass().getName(), column)
        );
      }

      byte[] out = new byte[0];
      if (type == Type.BASE32) {
        out = base32Encode.decode(value);
      } else if (type == Type.BASE64) {
        out = base64Encode.decode(value);
      } else if (type == Type.HEX) {
        try {
          out = hexEncode.decode(value);
        } catch (DecoderException e) {
          throw new StepException(
            String.format("%s : Failed to decode hex value.", toString())
          );
        }
      } else {
        throw new StepException(
          String.format("%s : Invalid type of encoding '%s' specified", toString(), type.toString())
        );
      }

      String obj = new String(out, StandardCharsets.UTF_8);
      record.addOrSet(String.format("%s_decode_%s", column, type.toString().toLowerCase(Locale.ENGLISH)), obj);
    }
    return records;
  }
}
