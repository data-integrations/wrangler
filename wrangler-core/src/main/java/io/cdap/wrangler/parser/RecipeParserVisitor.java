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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.parser.grammar.DirectivesParser;
import io.cdap.wrangler.parser.grammar.DirectivesBaseVisitor;

public class RecipeParserVisitor extends DirectivesBaseVisitor<Token> {

  @Override
  public Token visitByteSize(DirectivesParser.ByteSizeContext ctx) {
    return new ByteSize(ctx.getText());
  }

  @Override
  public Token visitTimeDuration(DirectivesParser.TimeDurationContext ctx) {
    return new TimeDuration(ctx.getText());
  }
} 