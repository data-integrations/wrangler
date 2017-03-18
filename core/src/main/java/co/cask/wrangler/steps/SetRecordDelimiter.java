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

import java.util.ArrayList;
import java.util.List;

/**
 * A step for parsing a string into record using the record delimiter.
 */
@Usage(
  directive = "set-record-delim",
  usage = "set-record-delim <column> <delimiter> [<limit>]",
  description = "Sets the record delimiter."
)
public class SetRecordDelimiter extends AbstractIndependentStep {
  private static final Messages MSG = MessagesFactory.getMessages();
  private final String column;
  private final String delimiter;
  private final int limit;

  public SetRecordDelimiter(int lineno, String detail, String column, String delimiter, int limit) {
    super(lineno, detail, column);
    this.column = column;
    this.delimiter = delimiter;
    this.limit = limit;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context)
    throws StepException, ErrorRecordException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(column);
      if (idx == -1) {
        continue;
      }

      Object object = record.getValue(idx);
      if (object instanceof String) {
        String body = (String) object;
        String[] lines = body.split(delimiter);
        int i = 0;
        for (String line : lines) {
          if (i > limit) {
            break;
          }
          results.add(new Record(column, line));
          i++;
        }
      }
    }
    return results;
  }
}
