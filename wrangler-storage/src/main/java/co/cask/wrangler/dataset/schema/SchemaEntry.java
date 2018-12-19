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

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * Schema Entry
 */
public final class SchemaEntry {
  private final String id;
  private final String name;
  private final String description;
  private final SchemaDescriptorType type;
  private final Set<Long> versions;
  private final byte[] specification;
  private final long current;

  public SchemaEntry(String id, String name, String description, SchemaDescriptorType type,
                     Set<Long> versions, byte[] specification, long current) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.type = type;
    this.versions = versions;
    this.specification = specification;
    this.current = current;
  }

  public String getId() {
    return id;
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

  public byte[] getSpecification() {
    return specification;
  }

  public long getCurrent() {
    return current;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaEntry that = (SchemaEntry) o;
    return current == that.current &&
      Objects.equals(id, that.id) &&
      Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      type == that.type &&
      Objects.equals(versions, that.versions) &&
      Arrays.equals(specification, that.specification);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id, name, description, type, versions, current);
    result = 31 * result + Arrays.hashCode(specification);
    return result;
  }
}
