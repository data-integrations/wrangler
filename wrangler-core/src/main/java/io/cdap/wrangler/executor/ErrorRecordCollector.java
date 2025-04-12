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

package io.cdap.wrangler.executor;

import io.cdap.wrangler.api.ErrorRecord;
import io.cdap.wrangler.api.annotations.Public;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a error collector, a collection of all the records that are errored.
 */
@Public
public final class ErrorRecordCollector {
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
