/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.proto;

import io.cdap.wrangler.api.ErrorRecordBase;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An exception that contains error records.
 */
public class ErrorRecordsException extends RuntimeException {
  static final int MAX_NUM_ERRORS_IN_SUMMARY = 10;
  static final String ERROR_SUMMARY_ITEM_DELIM = "\n";

  private final List<ErrorRecordBase> errorRecords;

  public ErrorRecordsException(List<ErrorRecordBase> errorRecords) {
    super(summarize(errorRecords));
    this.errorRecords = errorRecords;
  }

  public List<ErrorRecordBase> getErrorRecords() {
    return errorRecords;
  }

  static String summarize(List<ErrorRecordBase> errorRecords) {
    List<String> lines = errorRecords
      .stream()
      // Group similar conformance issues by hashing them.
      .collect(Collectors.groupingBy(ErrorRecordBase::getMessage))
      .entrySet()
      .stream()
      // Most common issues come first.
      .sorted(Comparator.comparingInt(a -> -a.getValue().size()))
      // Create one string per unique error.
      .map(entry -> String.format("%s - %d rows", entry.getKey(), entry.getValue().size()))
      .collect(Collectors.toList());

    if (lines.size() > MAX_NUM_ERRORS_IN_SUMMARY) {
      lines = lines.subList(0, MAX_NUM_ERRORS_IN_SUMMARY - 1);
      lines.add("... and other errors hidden for brevity");
    }

    return String.join(ERROR_SUMMARY_ITEM_DELIM, lines);
  }
}
