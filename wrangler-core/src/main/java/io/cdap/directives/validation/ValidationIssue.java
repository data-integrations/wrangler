/*
 *  Copyright © 2019 Google Inc.
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

package io.cdap.directives.validation;

import edu.emory.mathcs.backport.java.util.Arrays;
import edu.emory.mathcs.backport.java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class ValidationIssue {
  private String schemaLocation;
  private String dataLocation;
  private String error;
  private List<UUID> rowIds;

  public ValidationIssue(String schemaLocation, String dataLocation, String error, UUID... rowIds) {
    this.schemaLocation = schemaLocation;
    this.dataLocation = dataLocation;
    this.error = error;
    this.rowIds = Collections.unmodifiableList(Arrays.asList(rowIds));
  }

  public String getSchemaLocation() {
    return schemaLocation;
  }

  public String getDataLocation() {
    return dataLocation;
  }

  public String getError() {
    return error;
  }

  public String toString() {
    return String
        .format("error at schema %s, at data %s: %s", getSchemaLocation(), getDataLocation(), getError());
  }

  public List<UUID> getRowIds() {
    return rowIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValidationIssue that = (ValidationIssue) o;
    return schemaLocation.equals(that.schemaLocation) &&
        dataLocation.equals(that.dataLocation) &&
        error.equals(that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaLocation, dataLocation, error);
  }
}
