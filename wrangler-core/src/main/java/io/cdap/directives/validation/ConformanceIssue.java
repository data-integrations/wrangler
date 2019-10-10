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

package io.cdap.directives.validation;

import java.util.Objects;

/**
 * Contains information about an issue that occurred when trying to validate some data (against a schema).
 */
public class ConformanceIssue {

  /**
   * Schema location refers to the part of the schema where this issue occurred. The exact format varies by schema
   * implementation. For JSON Schema for example, it may look like #/definitions/Blue.
   */
  private final String schemaLocation;

  /**
   * Data location refers to the part of the input data where this issue occurred. The exact format varies by conformer
   * implementation. This may be a dot/bracket notation style path or something similar.
   */
  private final String dataLocation;


  /**
   * A human readable description of why the data didn't conform.
   */
  private final String error;

  public ConformanceIssue(String schemaLocation, String dataLocation, String error) {
    this.schemaLocation = schemaLocation;
    this.dataLocation = dataLocation;
    this.error = error;
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

  @Override
  public String toString() {
    String error = getError();
    String dataLocation = getDataLocation();
    if (error.contains(dataLocation)) {
      return String.format("error at schema %s: %s", getSchemaLocation(), error);
    }
    return String.format(
      "error at schema %s, at data %s: %s", getSchemaLocation(), dataLocation, error);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConformanceIssue that = (ConformanceIssue) o;
    return Objects.equals(schemaLocation, that.schemaLocation)
      && Objects.equals(dataLocation, that.dataLocation)
      && Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaLocation, dataLocation, error);
  }
}
