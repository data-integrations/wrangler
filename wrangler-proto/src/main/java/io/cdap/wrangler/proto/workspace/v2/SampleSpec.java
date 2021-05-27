/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.proto.workspace.v2;

import io.cdap.cdap.api.data.schema.Schema;

import java.util.Map;
import java.util.Objects;

/**
 * Spec for the workspace sample
 */
public class SampleSpec {
  private final String connectionName;
  private final String path;
  private final Schema sampleSchema;
  private final Map<String, String> properties;

  public SampleSpec(String connectionName, String path, Schema sampleSchema, Map<String, String> properties) {
    this.connectionName = connectionName;
    this.path = path;
    this.sampleSchema = sampleSchema;
    this.properties = properties;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public String getPath() {
    return path;
  }

  public Schema getSampleSchema() {
    return sampleSchema;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SampleSpec that = (SampleSpec) o;
    return Objects.equals(connectionName, that.connectionName) &&
             Objects.equals(path, that.path) &&
             Objects.equals(sampleSchema, that.sampleSchema) &&
             Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionName, path, sampleSchema, properties);
  }
}
