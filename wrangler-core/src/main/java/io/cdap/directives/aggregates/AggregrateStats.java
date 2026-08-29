/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

 package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.Token;

import java.util.List;
import java.util.stream.Collectors;

public class AggregateStats implements Directive {

  private String operation;

  @Override
  public void initialize(List<Token> args) {
    for (Token token : args) {
      if (token instanceof Text) {
        Object val = ((Text) token).value();
        if (val instanceof String) {
          operation = ((String) val).toLowerCase();
        }
      }
    }
  }

  @Override
  public List<Row> execute(List<Row> rows) {
    // Dummy implementation
    return rows;
  }

  @Override
  public void destroy() {
    // No cleanup needed
  }

  // Optionally implement define() if required by your Directive interface
  // If the Directive interface changed, add the appropriate method override
}