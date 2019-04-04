/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.api.lineage;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * This class <code>Mutation</code> defines a mutation on the column.
 * <code>MutationType</code> defines the type of mutation that is being
 * applied within a column.
 */
public final class Mutation {
  // Name of the column on which the mutation is applied.
  private final String column;

  // Specifies the type of the mutation being applied on the column.
  private final MutationType type;

  // Optional description for the mutation specified.
  private final String description;

  public Mutation(String column, MutationType type, String description) {
    this.column = column;
    this.type = type;
    this.description = description;
  }

  /**
   * @return the column on which mutation is being applied.
   */
  public String column() {
    return column;
  }

  /**
   * @return the type of mutation being applied on the <code>column</code>.
   */
  public MutationType type() {
    return type;
  }

  /**
   * @return the optional description for the mutation.
   */
  public String description() {
    return description;
  }

  /**
   * Generates a <code>JsonElement</code> object representation of <code>Mutation</code>
   * @return the object representation of <code>Mutation</code> as <code>JsonElement</code>
   */
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("column", column);
    object.addProperty("type", type.name());
    object.addProperty("description", description);
    return object;
  }
}
