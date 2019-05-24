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

package io.cdap.wrangler.dataset.workspace;

import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.Request;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Contains all information about a workspace.
 */
public class Workspace extends WorkspaceMeta {
  private final NamespacedId id;
  private final long created;
  private final long updated;
  private final byte[] data;
  private final Request request;

  private Workspace(NamespacedId id, String name, String scope, DataType type, Map<String, String> properties,
                    long created, long updated, @Nullable byte[] data, @Nullable Request request) {
    super(name, scope, type, properties);
    this.id = id;
    this.created = created;
    this.updated = updated;
    this.data = data == null ? null : Arrays.copyOf(data, data.length);
    this.request = request;
  }

  public NamespacedId getNamespacedId() {
    return id;
  }

  public long getCreated() {
    return created;
  }

  public long getUpdated() {
    return updated;
  }

  @Nullable
  public byte[] getData() {
    return data;
  }

  @Nullable
  public Request getRequest() {
    return request;
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
    Workspace workspace = (Workspace) o;
    return created == workspace.created &&
      updated == workspace.updated &&
      Objects.equals(id, workspace.id) &&
      Arrays.equals(data, workspace.data) &&
      Objects.equals(request, workspace.request);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), id, created, updated, request);
    result = 31 * result + Arrays.hashCode(data);
    return result;
  }

  public static Builder builder(NamespacedId id, String name) {
    return new Builder(id, name);
  }

  public static Builder builder(Workspace existing) {
    return new Builder(existing.getNamespacedId(), existing.getName())
      .setType(existing.getType())
      .setScope(existing.getScope())
      .setCreated(existing.getCreated())
      .setUpdated(existing.getUpdated())
      .setProperties(existing.getProperties())
      .setData(existing.getData())
      .setRequest(existing.getRequest());
  }

  /**
   * Creates Workspace objects.
   */
  public static class Builder extends WorkspaceMeta.Builder<Builder> {
    private final NamespacedId id;
    private long created;
    private long updated;
    private byte[] data;
    private Request request;

    Builder(NamespacedId id, String name) {
      super(name);
      this.id = id;
    }

    public Builder setCreated(long created) {
      this.created = created;
      return this;
    }

    public Builder setUpdated(long updated) {
      this.updated = updated;
      return this;
    }

    public Builder setData(byte[] data) {
      this.data = data;
      return this;
    }

    public Builder setRequest(Request request) {
      this.request = request;
      return this;
    }

    public Workspace build() {
      return new Workspace(id, name, scope, type, properties, created, updated, data, request);
    }
  }
}
