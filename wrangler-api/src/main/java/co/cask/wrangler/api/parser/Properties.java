/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.annotations.PublicEvolving;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;

/**
 * Class description here.
 */
@PublicEvolving
public class Properties implements Token {
  private Map<String, Token> values;

  public Properties(Map<String, Token> values) {
    this.values = values;
  }

  @Override
  public Map<String, Token> value() {
    return values;
  }

  @Override
  public TokenType type() {
    return TokenType.PROPERTIES;
  }

  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    for(Map.Entry<String, Token> entry : values.entrySet()) {
      object.add(entry.getKey(), entry.getValue().toJson());
    }
    return object;
  }
}
