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

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ErrorRecordsExceptionTest {

  @Test
  public void summarizeLimitsLargeLists() {
    List<ErrorRecordBase> errs = IntStream.range(0, 100)
        .boxed()
        .map(i -> new ErrorRecordBase(String.format("error # %d", i), i, true))
        .collect(Collectors.toList());

    String result = ErrorRecordsException.summarize(errs);

    Assert.assertEquals("wrong number of summary entries",
        ErrorRecordsException.MAX_NUM_ERRORS_IN_SUMMARY,
        result.split(ErrorRecordsException.ERROR_SUMMARY_ITEM_DELIM).length);
    Assert.assertTrue(String.format("result had wrong ending: %s", result),
        result.endsWith("for brevity"));
  }

  @Test
  public void summarizeLeavesMaxSizedLists() {
    List<ErrorRecordBase> errs = IntStream
        .range(1, ErrorRecordsException.MAX_NUM_ERRORS_IN_SUMMARY + 1)
        .boxed()
        .map(i -> new ErrorRecordBase(String.format("error # %d", i), i, true))
        .collect(Collectors.toList());

    String result = ErrorRecordsException.summarize(errs);

    Assert.assertEquals("wrong number of summary entries",
        ErrorRecordsException.MAX_NUM_ERRORS_IN_SUMMARY,
        result.split(ErrorRecordsException.ERROR_SUMMARY_ITEM_DELIM).length);
  }

  @Test
  public void summarizeCollapsesRepeatedErrors() {
    List<ErrorRecordBase> errs = IntStream.range(1, 102)
        .boxed()
        .map(i -> new ErrorRecordBase(String.format("error %s", i % 2 == 0 ? "foo" : "bar"), i,
            true))
        .collect(Collectors.toList());

    String[] results = ErrorRecordsException.summarize(errs)
        .split(ErrorRecordsException.ERROR_SUMMARY_ITEM_DELIM);

    Assert.assertArrayEquals(new String[]{"error bar - 51 rows", "error foo - 50 rows"}, results);
  }
}
