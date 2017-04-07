package co.cask.wrangler.api;

/**
 * Class description here.
 */
public final class ErrorRecord {
  private final Record record;
  private final String message;
  private final int code;

  public ErrorRecord(Record record, String message, int code) {
    this.record = record;
    this.message = message;
    this.code = code;
  }

  public Record getRecord() {
    return record;
  }

  public String getMessage() {
    return message;
  }

  public int getCode() {
    return code;
  }
}
