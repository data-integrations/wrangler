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

import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.schema.SchemaDescriptorType;

/**
 * Describes a schema.
 */
public class SchemaDescriptor {
  private final NamespacedId id;
  private final String name;
  private final String description;
  private final SchemaDescriptorType type;

  public SchemaDescriptor(NamespacedId id, String name, String description, SchemaDescriptorType type) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.type = type;
  }

  public NamespacedId getId() {
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
}
