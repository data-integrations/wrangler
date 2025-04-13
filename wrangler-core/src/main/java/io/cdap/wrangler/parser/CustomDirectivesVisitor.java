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
import io.cdap.wrangler.api.parser.Token;

/**
 * Custom visitor that extends the generated DirectivesBaseVisitor.
 * This visitor overrides the visitValue method to recognize byte size
 * and time duration tokens.
 */
public class CustomDirectivesVisitor extends DirectivesBaseVisitor<Token> {

    /**
     * Overrides visitValue to detect if the token represents a byte size (e.g., "10KB")
     * or a time duration (e.g., "150ms"). If it matches either pattern, it creates the
     * appropriate token. Otherwise, it delegates to the default behavior.
     *
     * @param ctx the parse tree context for a value.
     * @return a Token representing the parsed value.
     */
    @Override
    public Token visitValue(DirectivesParser.ValueContext ctx) {
        String text = ctx.getText();
        
        // Check if the text matches a byte size pattern
        if (text.matches("[0-9]+(\\.[0-9]+)?(B|KB|MB|GB|TB)")) {
            return new ByteSize(text);
        }
        // Check if the text matches a time duration pattern
        else if (text.matches("[0-9]+(\\.[0-9]+)?(ms|s|m|h)")) {
            return new TimeDuration(text);
        }
        // Otherwise, fallback to the default implementation
        return super.visitValue(ctx);
    }
}
