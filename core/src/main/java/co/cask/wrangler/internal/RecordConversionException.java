package co.cask.wrangler.internal;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.wrangler.api.Record;

/**
 * Throw when there is issue with conversion of {@link Record} to {@link StructuredRecord}
 */
public class RecordConversionException extends Exception {
  public RecordConversionException(String message) {
    super(message);
  }
}
