/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.wrangler.dataset.schema;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.wrangler.proto.schema.SchemaDescriptorType;

import javax.annotation.Nullable;

/**
 * Contains all the information about a schema that is stored in the same row.
 */
public class SchemaRow extends SchemaDescriptor {
  private static final byte[] NAME_COL = Bytes.toBytes("name");
  private static final byte[] DESC_COL = Bytes.toBytes("description");
  private static final byte[] CREATED_COL = Bytes.toBytes("created");
  private static final byte[] UPDATED_COL = Bytes.toBytes("updated");
  private static final byte[] TYPE_COL = Bytes.toBytes("type");
  private static final byte[] AUTO_VERSION_COL = Bytes.toBytes("auto");
  private static final byte[] CURRENT_VERSION_COL = Bytes.toBytes("current");

  private final long created;
  private final long updated;
  private final long autoVersion;
  private final Long currentVersion;

  private SchemaRow(String id, String name, String description, SchemaDescriptorType type,
                    long created, long updated, long autoVersion, @Nullable Long currentVersion) {
    super(id, name, description, type);
    this.created = created;
    this.updated = updated;
    this.autoVersion = autoVersion;
    this.currentVersion = currentVersion;
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

  Put toPut() {
    Put put = new Put(id);
    put.add(NAME_COL, name);
    put.add(DESC_COL, description);
    put.add(CREATED_COL, created);
    put.add(UPDATED_COL, updated);
    put.add(TYPE_COL, type.name());
    put.add(AUTO_VERSION_COL, autoVersion);
    if (currentVersion != null) {
      put.add(CURRENT_VERSION_COL, currentVersion);
    }
    return put;
  }

  static SchemaRow fromRow(Row row) {
    String id = Bytes.toString(row.getRow());
    String name = row.getString(NAME_COL);
    String description = row.getString(DESC_COL);
    String typeStr = row.getString(TYPE_COL);
    SchemaDescriptorType type = SchemaDescriptorType.valueOf(typeStr);
    long created = row.getLong(CREATED_COL);
    long updated = row.getLong(UPDATED_COL);
    long auto = row.getLong(AUTO_VERSION_COL);
    Long current = row.getLong(CURRENT_VERSION_COL);
    return new SchemaRow(id, name, description, type, created, updated, auto, current);
  }

  static Builder builder(SchemaRow existing) {
    return new Builder(existing.getId())
      .setName(existing.getName())
      .setDescription(existing.getDescription())
      .setType(existing.getType())
      .setUpdated(existing.getUpdated())
      .setAutoVersion(existing.getAutoVersion())
      .setCurrentVersion(existing.getCurrentVersion());
  }

  static Builder builder(SchemaDescriptor descriptor) {
    return new Builder(descriptor.getId())
      .setName(descriptor.getName())
      .setDescription(descriptor.getDescription())
      .setType(descriptor.getType());
  }

  /**
   * Builds a SchemaRow.
   */
  public static class Builder {
    private final String id;
    private String name;
    private String description;
    private SchemaDescriptorType type;
    private long created;
    private long updated;
    private long autoVersion;
    private Long currentVersion;

    public Builder(String id) {
      this.id = id;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setType(SchemaDescriptorType type) {
      this.type = type;
      return this;
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
      return new SchemaRow(id, name, description, type, created, updated, autoVersion, currentVersion);
    }
  }
}
