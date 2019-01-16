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

package co.cask.wrangler.dataset.workspace;

import co.cask.wrangler.proto.NamespacedId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata about a workspace.
 */
public class WorkspaceMeta extends NamespacedId {
  private final String name;
  private final String scope;
  private final DataType type;
  private final Map<String, String> properties;

  protected WorkspaceMeta(NamespacedId id, String name, String scope, DataType type, Map<String, String> properties) {
    super(id);
    this.name = name;
    this.scope = scope;
    this.type = type;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  public String getName() {
    return name;
  }

  public String getScope() {
    return scope;
  }

  public DataType getType() {
    return type;
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
    if (!super.equals(o)) {
      return false;
    }
    WorkspaceMeta that = (WorkspaceMeta) o;
    return Objects.equals(scope, that.scope) &&
      type == that.type &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), scope, type, properties);
  }

  public static Builder builder(NamespacedId id, String name) {
    return new Builder(id, name);
  }

  /**
   * Creates a WorkspaceMeta instance.
   *
   * @param <T> type of builder
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T extends Builder> {
    protected final NamespacedId id;
    protected final String name;
    protected String scope;
    protected DataType type;
    protected Map<String, String> properties;

    Builder(NamespacedId id, String name) {
      this.id = id;
      this.name = name;
      this.properties = new HashMap<>();
      this.scope = WorkspaceDataset.DEFAULT_SCOPE;
      this.type = DataType.BINARY;
    }

    public T setScope(String scope) {
      this.scope = scope;
      return (T) this;
    }

    public T setType(DataType type) {
      this.type = type;
      return (T) this;
    }

    public T setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return (T) this;
    }

    public WorkspaceMeta build() {
      return new WorkspaceMeta(id, name, scope, type, properties);
    }
  }
}
