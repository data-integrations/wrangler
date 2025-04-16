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

package io.cdap.wrangler.proto;

import java.util.Objects;

/**
 * Specification about workspace for the {@link Request}
 */
public final class Workspace {
  // Name of the workspace.
  private final String name;

  // Number of results to be returned for the workspace.
  private final Integer results;

  public Workspace(String name, Integer results) {
    this.name = name;
    this.results = results;
  }

  /**
   * @return Name of the workspace.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Number of results to return in the API.
   */
  public Integer getResults() {
    return results;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Workspace workspace = (Workspace) o;
    return Objects.equals(name, workspace.name) &&
      Objects.equals(results, workspace.results);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, results);
  }
}
