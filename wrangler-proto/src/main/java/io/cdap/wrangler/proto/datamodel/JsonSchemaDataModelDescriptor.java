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

package io.cdap.wrangler.proto.datamodel;

import io.cdap.wrangler.proto.NamespacedId;

/**
 * A utility class to be used for querying details about a {@JsonSchema}.
 */
public final class JsonSchemaDataModelDescriptor extends AbstractDataModelDescriptor<JsonSchemaDataModel> {

  public JsonSchemaDataModelDescriptor(NamespacedId id, JsonSchemaDataModel schema, DataModelType dataModelType) {
    super(id, schema, dataModelType);
  }

  /**
   * Builder for constructing a {@JsonSchemaDataModelDescriptor} from an existing definition.
   * @param existing
   * @return a builder initialized with the options of an existing {@JsonSchemaDataModelDescriptor}
   */
  public static Builder builder(JsonSchemaDataModelDescriptor existing) {
    return new Builder()
        .setId(existing.getNamespacedId())
        .setJsonSchemaDataModel(existing.getDataModel());
  }
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a JsonSchemaDataModelDescriptor
   */
  public static class Builder {
    private NamespacedId id;
    private JsonSchemaDataModel jsonSchemaDataModel;

    Builder() {}

    public Builder setId(NamespacedId id) {
      this.id = id;
      return this;
    }

    public Builder setJsonSchemaDataModel(JsonSchemaDataModel jsonSchemaDataModel) {
      this.jsonSchemaDataModel = jsonSchemaDataModel;
      return this;
    }

    public JsonSchemaDataModelDescriptor build() {
      return new JsonSchemaDataModelDescriptor(this.id, this.jsonSchemaDataModel, DataModelType.JSON_SCHEMA);
    }
  }
}

