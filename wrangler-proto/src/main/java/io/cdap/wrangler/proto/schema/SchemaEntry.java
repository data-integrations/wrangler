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

package io.cdap.wrangler.proto.schema;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.wrangler.proto.NamespacedId;

import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Schema Entry.
 *
 * TODO: (CDAP-14679) Create another SchemaInfo class for the schema itself
 *   and remove version list and current field from this class.
 */
public final class SchemaEntry extends NamespacedId {
  private final String name;
  private final String description;
  private final SchemaDescriptorType type;
  private final Set<Long> versions;
  private final String specification;
  private final Long current;

  public SchemaEntry(NamespacedId id, String name, String description, SchemaDescriptorType type,
                     Set<Long> versions, @Nullable byte[] specification, @Nullable Long current) {
    super(id);
    this.name = name;
    this.description = description;
    this.type = type;
    this.versions = versions;
    this.specification = specification == null ? null : Bytes.toHexString(specification);
    this.current = current;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public SchemaDescriptorType getType() {
    return type;
  }

  public Set<Long> getVersions() {
    return versions;
  }

  @Nullable
  public Long getCurrent() {
    return current;
  }

  @Nullable
  public byte[] getSpecification() {
    return specification == null ? null : Bytes.fromHexString(specification);
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
    SchemaEntry that = (SchemaEntry) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      type == that.type &&
      Objects.equals(versions, that.versions) &&
      Objects.equals(specification, that.specification) &&
      Objects.equals(current, that.current);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name, description, type, versions, specification, current);
  }
}
