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

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

public class CustomDirectivesVisitor extends DirectivesBaseVisitor<Token> {
    
    @Override
    public ByteSize visitByteSizeArg(DirectivesParser.ByteSizeArgContext ctx) {
        return new ByteSize(ctx.getText());
    }

    @Override
    public TimeDuration visitTimeDurationArg(DirectivesParser.TimeDurationArgContext ctx) {
        return new TimeDuration(ctx.getText());
    }
    
    // Add this method to handle Token conversion if needed
    private Token convertToToken(Object value) {
        // Implement your custom conversion logic here
        // For example, if you need to wrap ByteSize/TimeDuration in a Token
        return new YourCustomTokenImplementation(value);
    }
}