/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.SourceInfo;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;

import java.util.HashMap;
import java.util.Map;

/**
 * TestArguments is used to pass dummy arguments during testing of directives.
 */
public class TestArguments implements Arguments {

  private final Map<String, Token> args = new HashMap<>();

  // Constructor for passing single argument
  public TestArguments(String columnValue) {
    args.put("column", new Text(columnValue));
  }

  @Override
  public <T extends Token> T value(String key) {
    return (T) args.get(key);
  }

  @Override
  public boolean contains(String key) {
    return args.containsKey(key);
  }

  @Override
  public int size() {
    return args.size();
  }

  @Override
  public JsonElement toJson() {
    return JsonNull.INSTANCE;
  }

  @Override
  public int column() {
    return 0; // Return any int value as per your need
  }

  @Override
  public int line() {
    return 0;
  }

  @Override
  public TokenType type(String key) {
    return null;
  }

  @Override
  public String source() {
    return "test-source";
  }
}
