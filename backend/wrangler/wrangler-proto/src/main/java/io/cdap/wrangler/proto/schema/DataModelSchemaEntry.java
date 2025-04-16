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
package io.cdap.wrangler.proto.schema;

import io.cdap.wrangler.proto.NamespacedId;

import java.util.Objects;

/**
 * DataModelSchemaEntry represents the meta data of a data model.
 */
public final class DataModelSchemaEntry {

  private final NamespacedId namespacedId;
  private final String displayName;
  private final String description;
  private final Long revision;

  public DataModelSchemaEntry(NamespacedId namespacedId, String displayName, String description, Long revision) {
    this.namespacedId = namespacedId;
    this.displayName = displayName;
    this.description = description;
    this.revision = revision;
  }

  public NamespacedId getNamespacedId() {
    return namespacedId;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDescription() {
    return description;
  }

  public Long getRevision() {
    return revision;
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
    DataModelSchemaEntry that = (DataModelSchemaEntry) o;
    return Objects.equals(namespacedId, that.namespacedId) &&
      Objects.equals(displayName, that.displayName) &&
      Objects.equals(description, that.description) &&
      Objects.equals(revision, that.revision);
  }
}
