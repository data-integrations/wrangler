package co.cask.wrangler.api;

/**
 * Exception throw when the record needs to emitted to error collector.
 */
public class ErrorRecordException extends Exception {
  // Message as to why the record errored.
  private String message;

  // Code associated with the error message.
  private int code;

  public ErrorRecordException(String message, int code) {
    this.message = message;
    this.code = code;
  }

  /**
   * @return Message as why the record errored.
   */
  public String getMessage() {
    return message;
  }

  /**
   * @return code related to the message.
   */
  public int getCode() {
    return code;
  }
}
