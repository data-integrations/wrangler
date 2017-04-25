package co.cask.wrangler.api;

import co.cask.wrangler.api.annotations.Public;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a error collector, a collection of all the records that are errored.
 */
@Public
public class ErrorRecordCollector {
  // Array of records that are erroed.
  private final List<ErrorRecord> errors;

  public ErrorRecordCollector() {
    errors = new ArrayList<>();
  }

  /**
   * @return Size of the errored list.
   */
  public int size() {
    return errors.size();
  }

  /**
   * Resets the error list.
   */
  public void reset() {
    errors.clear();
  }

  /**
   * Adds a {@link ErrorRecord} to the error collector.
   *
   * @param record
   */
  public void add(ErrorRecord record) {
    errors.add(record);
  }

  /**
   * @return List of errors.
   */
  public List<ErrorRecord> get() {
    return errors;
  }
}
