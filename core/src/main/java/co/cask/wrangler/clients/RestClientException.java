package co.cask.wrangler.clients;

/**
 * Class description here.
 */
public class RestClientException extends Exception {

  private final int status;
  private final int errorCode;

  public RestClientException(final String message, final int status, final int errorCode) {
    super(message + "; error code: " + errorCode);
    this.status = status;
    this.errorCode = errorCode;
  }

  public int getStatus() {
    return status;
  }

  public int getErrorCode() {
    return errorCode;
  }
}