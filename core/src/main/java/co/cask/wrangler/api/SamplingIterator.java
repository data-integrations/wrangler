package co.cask.wrangler.api;

/**
 * Class description here.
 */

import java.util.Iterator;

/**
 * A simple abstract iterator which implements the remove method as unsupported operation.
 *
 * @param <T> The type of iterator data.
 */
public abstract class SamplingIterator<T> implements Iterator<T> {
  @Override
  public void remove() {
    throw new UnsupportedOperationException("This operation is not supported.");
  }
}
