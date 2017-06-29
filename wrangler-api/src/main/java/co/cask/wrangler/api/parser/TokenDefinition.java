/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.annotations.PublicEvolving;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.Serializable;

/**
 * Class description here.
 */
@PublicEvolving
public final class TokenDefinition implements Serializable {
  private final int ordinal;
  private final boolean optional;
  private final String name;
  private final TokenType type;
  private final String label;

  public TokenDefinition(String name, TokenType type, String label, int ordinal, boolean optional) {
    this.name = name;
    this.type = type;
    this.label = label;
    this.ordinal = ordinal;
    this.optional = optional;
  }

  public String label() {
    return label;
  }

  public int ordinal() {
    return ordinal;
  }

  public boolean optional() {
    return optional;
  }

  public String name() {
    return name;
  }

  public TokenType type() {
    return type;
  }

  public JsonElement toJsonObject() {
    JsonObject object = new JsonObject();
    object.addProperty("name", name);
    object.addProperty("type", type.name());
    object.addProperty("ordinal", ordinal);
    object.addProperty("optional", optional);
    object.addProperty("label", label);
    return object;
  }
}
