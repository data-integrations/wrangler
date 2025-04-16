/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.service.explorer;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class provides an wrapper over the {@link InputStream} that limits the number of lines being
 * read from the file.
 *
 * Uses a <code>BufferedReader</code> internally to read the data. If the number of lines in file is
 * fewer than the lines in file, it will return only the lines that can be possibly read.
 */
public final class BoundedLineInputStream implements Iterator<String>, Closeable {
  // The reader that is being read.
  private final BufferedReader bufferedReader;

  // The current line.
  private String cachedLine;

  // A flag indicating if the iterator has been fully read.
  private boolean finished = false;

  // Number of lines to be read.
  private int lines;

  /**
   * Constructs an iterator of the lines for a <code>Reader.
   *
   * @param reader the <code>Reader to read from, not null
   * @throws IllegalArgumentException if the reader is null
   */
  public BoundedLineInputStream(final Reader reader, int lines) throws IllegalArgumentException {
    if (reader == null) {
      throw new IllegalArgumentException("Reader must not be null");
    }
    if (reader instanceof BufferedReader) {
      bufferedReader = (BufferedReader) reader;
    } else {
      bufferedReader = new BufferedReader(reader);
    }
    this.lines = lines;
  }

  /**
   * Indicates whether the <code>Reader has more lines.
   * If there is an <code>IOException then {@link #close()} will
   * be called on this instance.
   *
   * @return <code>true if the Reader has more lines
   * @throws IllegalStateException if an IO exception occurs
   */
  public boolean hasNext() {
    if (cachedLine != null) {
      return true;
    } else if (finished) {
      return false;
    } else {
      try {
        while (true) {
          if (lines == 0) {
            finished = true;
            return false;
          }
          String line = bufferedReader.readLine();
          if (line == null) {
            finished = true;
            return false;
          } else if (isValidLine(line)) {
            cachedLine = line;
            return true;
          }
        }
      } catch (IOException ioe) {
        close();
        throw new IllegalStateException(ioe);
      }
    }
  }

  /**
   * Overridable method to validate each line that is returned.
   *
   * @param line  the line that is to be validated
   * @return true if valid, false to remove from the iterator
   */
  protected boolean isValidLine(String line) {
    return true;
  }

  /**
   * Returns the next line in the wrapped <code>Reader.
   *
   * @return the next line from the input
   * @throws NoSuchElementException if there is no line to return
   */
  public String next() {
    return nextLine();
  }

  /**
   * Returns the next line in the wrapped <code>Reader.
   *
   * @return the next line from the input
   * @throws NoSuchElementException if there is no line to return
   */
  public String nextLine() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more lines");
    }
    String currentLine = cachedLine;
    cachedLine = null;
    lines--;
    return currentLine;
  }

  /**
   * Closes the underlying <code>Reader quietly.
   * This method is useful if you only want to process the first few
   * lines of a larger file. If you do not close the iterator
   * then the <code>Reader remains open.
   * This method can safely be called multiple times.
   */
  public void close() {
    finished = true;
    IOUtils.closeQuietly(bufferedReader);
    cachedLine = null;
  }

  /**
   * Unsupported.
   *
   * @throws UnsupportedOperationException always
   */
  public void remove() {
    throw new UnsupportedOperationException("Remove unsupported on LineIterator");
  }

  /**
   * Closes the iterator, handling null and ignoring exceptions.
   *
   * @param iterator  the iterator to close
   */
  public static void closeQuietly(BoundedLineInputStream iterator) {
    if (iterator != null) {
      iterator.close();
    }
  }

  /**
   * A static method to create instance of <code>BoundedLineInputStream</code>. This method
   * creates the instance specifying the character encoding and number of lines to be read.
   *
   * <p>The application using this method should ensure that they call <code>BoundedLineInputStream.close()</code>
   * once the application has finished reading. When 'lines' is greater than number of the files in the file, it
   * will be terminated once the end of the file is reached.</p>
   *
   * @param input Input stream.
   * @param encoding Type of encoding for the file.
   * @param lines number of lines to be read.
   * @return Iterator.
   * @throws IOException
   */
  public static BoundedLineInputStream iterator(InputStream input, Charset encoding, int lines)
    throws IOException {
    return new BoundedLineInputStream(new InputStreamReader(input, encoding), lines);
  }

  public static BoundedLineInputStream iterator(InputStream input, String encoding, int lines)
    throws IOException {
    return iterator(input, Charsets.toCharset(encoding), lines);
  }
}
