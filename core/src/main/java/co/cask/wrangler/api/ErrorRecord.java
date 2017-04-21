package co.cask.wrangler.api;

/**
 * Specifies the structure for Error records.
 */
@Public
public final class ErrorRecord {
  // Actual record that is errored.
  private final Record record;

  // Message as to why the record errored.
  private final String message;

  // Code associated with the message.
  private final int code;

  public ErrorRecord(Record record, String message, int code) {
    this.record = record;
    this.message = message;
    this.code = code;
  }

  /**
   * @return original {@link Record} that errored.
   */
  public Record getRecord() {
    return record;
  }

  /**
   * @return Message associated with the {@link Record}.
   */
  public String getMessage() {
    return message;
  }

  /**
   * @return Code associated with the error.
   */
  public int getCode() {
    return code;
  }
}
