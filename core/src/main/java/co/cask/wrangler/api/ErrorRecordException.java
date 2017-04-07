package co.cask.wrangler.api;

/**
 * Class description here.
 */
public class ErrorRecordException extends Exception {
  private String message;
  private int code;

  public ErrorRecordException(String message, int code) {
    this.message = message;
    this.code = code;
  }

  public String getMessage() {
    return message;
  }

  public int getCode() {
    return code;
  }
}
