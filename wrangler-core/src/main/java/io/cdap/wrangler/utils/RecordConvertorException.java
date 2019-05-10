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

package io.cdap.wrangler.utils;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.wrangler.api.Row;

/**
 * Throw when there is issue with conversion of {@link Row} to {@link StructuredRecord}
 */
public class RecordConvertorException extends Exception {
  public RecordConvertorException(String message) {
    super(message);
  }

  public RecordConvertorException(String message, Throwable cause) {
    super(message, cause);
  }
}
