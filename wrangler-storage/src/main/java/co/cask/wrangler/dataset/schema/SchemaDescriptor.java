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

import co.cask.wrangler.proto.schema.SchemaDescriptorType;

/**
 * Describes a schema.
 */
public class SchemaDescriptor {
  protected final String id;
  protected final String name;
  protected final String description;
  protected final SchemaDescriptorType type;

  public SchemaDescriptor(String id, String name, String description, SchemaDescriptorType type) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.type = type;
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
}
