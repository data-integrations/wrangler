package co.cask.wrangler.clients.entities;

/**
 *
 */
public final class ErrorMessage {
  private final int status;
  private final String message;

  public ErrorMessage(int status, String message) {
    this.status = status;
    this.message = message;
  }

  public int getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }
}
