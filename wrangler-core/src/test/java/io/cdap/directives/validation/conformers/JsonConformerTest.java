/*
 *  Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.validation.conformers;

import com.google.common.collect.Lists;
import io.cdap.directives.validation.ConformanceIssue;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.everit.json.schema.ValidationException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests JsonConformer. This does not test the checkConformance method as that simply delgates to the validator library
 * which has its own tests.
 */
@RunWith(JUnitParamsRunner.class)
public class JsonConformerTest {

  @Parameters
  private static Object[] convertValidationException_producesCorrectIssuesSummary_Parameters()
    throws JSONException {
    return new Object[]{
      new Object[]{
        "single (trivial) root exception",
        newValidationException("#", "#", "Oops"),
        Arrays.asList(new ConformanceIssue("#", "#", "{\"details\":\"here\"}"))
      },
      new Object[]{
        "single (non-trivial) exception",
        newValidationException(
          "#", "#", "Root oops", newValidationException("#/child", "#/child", "Child oops")),
        Arrays.asList(new ConformanceIssue("#/child", "#/child", "Child oops"))
      },
      new Object[]{
        "single (non-trivial) exception, along with root",
        newValidationException(
          "#",
          "#",
          "Root oops",
          newValidationException("#/child", "#/child", "Child oops"),
          newValidationException("#", "#", "Root oops")),
        Arrays.asList(new ConformanceIssue("#/child", "#/child", "Child oops"))
      },
      new Object[]{
        "multiple (non-trivial) exceptions",
        newValidationException(
          "#",
          "#",
          "Root oops",
          newValidationException("#/child", "#/child", "Child oops"),
          newValidationException("#/kid", "#/kid", "Kid oops"),
          newValidationException("#/moo", "#/moo", "Moo oops")),
        Arrays.asList(
          new ConformanceIssue("#/child", "#/child", "Child oops"),
          new ConformanceIssue("#/kid", "#/kid", "Kid oops"),
          new ConformanceIssue("#/moo", "#/moo", "Moo oops"))
      },
      new Object[]{
        "nested exceptions",
        newValidationException(
          "#",
          "#",
          "Root oops",
          newValidationException(
            "#/a",
            "#/a",
            "a oops",
            newValidationException(
              "#/b",
              "#/b",
              "b oops",
              newValidationException("#/c", "#/c", "c oops"),
              newValidationException("#/d", "#/d", "d oops")),
            newValidationException(
              "#/e",
              "#/e",
              "e oops",
              newValidationException("#/f", "#/f", "f oops"),
              newValidationException("#/g", "#/g", "g oops"))),
          newValidationException("#/h", "#/h", "h oops")),
        Arrays.asList(
          new ConformanceIssue("#/a -> #/b -> #/c", "#/c", "c oops"),
          new ConformanceIssue("#/a -> #/b -> #/d", "#/d", "d oops"),
          new ConformanceIssue("#/a -> #/e -> #/f", "#/f", "f oops"),
          new ConformanceIssue("#/a -> #/e -> #/g", "#/g", "g oops"),
          new ConformanceIssue("#/h", "#/h", "h oops"))
      },
    };
  }

  /**
   * Utility method for creating ValidationExceptions without access to the proper constructor.
   */
  private static ValidationException newValidationException(
    String schemaLocation, String dataLocation, String message, ValidationException... causes)
    throws JSONException {
    ValidationException m = mock(ValidationException.class);

    when(m.getSchemaLocation()).thenReturn(schemaLocation);
    when(m.getPointerToViolation()).thenReturn(dataLocation);
    when(m.getMessage()).thenReturn(message);
    when(m.getCausingExceptions()).thenReturn(Lists.newArrayList(causes));
    when(m.toJSON()).thenReturn(new JSONObject("{\"details\":\"here\"}"));

    return m;
  }

  @Test
  @Parameters(method = "convertValidationException_producesCorrectIssuesSummary_Parameters")
  public void convertValidationException_producesCorrectIssuesSummary(
    String name, ValidationException input, List<ConformanceIssue> want) {
    List<ConformanceIssue> got = JsonConformer.convertValidationException(input);
    assertArrayEquals(want.toArray(new ConformanceIssue[0]), got.toArray(new ConformanceIssue[0]));
  }
}
