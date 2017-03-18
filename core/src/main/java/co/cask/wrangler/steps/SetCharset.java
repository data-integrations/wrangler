package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractIndependentStep;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.api.i18n.Messages;
import co.cask.wrangler.api.i18n.MessagesFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Sets the character set encoding on the column.
 *
 * This directive will convert the data from {@link Byte[]} or {@link ByteBuffer}
 * to {@link String}. This conversion is through the character set encoding.
 */
@Usage(
  directive = "set-charset",
  usage = "set-charset <column> <charset>",
  description = "Sets the character set decoding to UTF-8."
)
public class SetCharset extends AbstractIndependentStep {
  private static final Messages MSG = MessagesFactory.getMessages();
  private final String column;
  private final String charset;

  public SetCharset(int lineno, String detail, String column, String charset) {
    super(lineno, detail, column);
    this.column = column;
    this.charset = charset;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException,
    ErrorRecordException {

    // Iterate through all the records.
    for (Record record : records) {
      int idx = record.find(column);
      if (idx == -1) {
        continue;
      }

      Object value = record.getValue(idx);
      if (value == null) {
        continue;
      }

      // Convert from byte[] or ByteBuffer into right ByteBuffer.
      ByteBuffer buffer;
      if (value instanceof byte[]) {
        buffer = ByteBuffer.wrap((byte[]) value);
      } else if (value instanceof ByteBuffer) {
        buffer = (ByteBuffer) value;
      } else {
        throw new StepException(
          MSG.get("")
        );
      }

      try {
        CharBuffer result = Charset.forName(charset).decode(buffer);
        record.setValue(idx, result.toString());
      } catch (Error e) {
        throw new StepException(
          MSG.get("")
        );
      }
    }

    return records;
  }
}
