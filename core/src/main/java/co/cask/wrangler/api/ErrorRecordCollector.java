package co.cask.wrangler.api;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
public class ErrorRecordCollector {
  private final List<ErrorRecord> errors;

  public ErrorRecordCollector() {
    errors = new ArrayList<>();
  }

  public int size() {
    return errors.size();
  }

  public void reset() {
    errors.clear();
  }

  public void add(ErrorRecord record) {
    errors.add(record);
  }

  public List<ErrorRecord> get() {
    return errors;
  }
}
