/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.wrangler.dataset.schema;

import javax.annotation.Nullable;

/**
 * Contains all the information about a schema that is stored in the same row.
 */
public class SchemaRow {

  private final SchemaDescriptor descriptor;
  private final long created;
  private final long updated;
  private final long autoVersion;
  private final Long currentVersion;

  private SchemaRow(SchemaDescriptor descriptor, long created, long updated, long autoVersion,
                    @Nullable Long currentVersion) {
    this.descriptor = descriptor;
    this.created = created;
    this.updated = updated;
    this.autoVersion = autoVersion;
    this.currentVersion = currentVersion;
  }

  public SchemaDescriptor getDescriptor() {
    return descriptor;
  }

  public long getCreated() {
    return created;
  }

  public long getUpdated() {
    return updated;
  }

  public long getAutoVersion() {
    return autoVersion;
  }

  @Nullable
  public Long getCurrentVersion() {
    return currentVersion;
  }

  static Builder builder(SchemaRow existing) {
    return new Builder(existing.getDescriptor())
      .setUpdated(existing.getUpdated())
      .setAutoVersion(existing.getAutoVersion())
      .setCurrentVersion(existing.getCurrentVersion());
  }

  static Builder builder(SchemaDescriptor descriptor) {
    return new Builder(descriptor);
  }

  /**
   * Builds a SchemaRow.
   */
  public static class Builder {
    private final SchemaDescriptor descriptor;
    private long created;
    private long updated;
    private long autoVersion;
    private Long currentVersion;

    public Builder(SchemaDescriptor descriptor) {
      this.descriptor = descriptor;
    }

    public Builder setCreated(long created) {
      this.created = created;
      return this;
    }

    public Builder setUpdated(long updated) {
      this.updated = updated;
      return this;
    }

    public Builder setAutoVersion(long autoVersion) {
      this.autoVersion = autoVersion;
      return this;
    }

    public Builder setCurrentVersion(Long currentVersion) {
      this.currentVersion = currentVersion;
      return this;
    }

    public SchemaRow build() {
      return new SchemaRow(descriptor, created, updated, autoVersion, currentVersion);
    }
  }
}
