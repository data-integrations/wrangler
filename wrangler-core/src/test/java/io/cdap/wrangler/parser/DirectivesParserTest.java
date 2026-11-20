/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import io.cdap.wrangler.api.parser.TokenType;
import org.antlr.v4.runtime.CharStreams;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DirectivesParserTest {

    @Test
    public void testParseByteSize() {
        String input = "10KB";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        DirectivesParser parser = new DirectivesParser(new CommonTokenStream(lexer));

        DirectivesParser.ValueContext context = parser.value();
        assertTrue(context.byteSize() != null);
        assertEquals("10KB", context.byteSize().getText());
    }

    @Test
    public void testParseTimeDuration() {
        String input = "1.5s";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        DirectivesParser parser = new DirectivesParser(new CommonTokenStream(lexer));

        DirectivesParser.ValueContext context = parser.value();
        assertTrue(context.timeDuration() != null);
        assertEquals("1.5s", context.timeDuration().getText());
    }

    @Test
    public void testParseMixedValues() {
        String input = "10KB 1.5s";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        DirectivesParser parser = new DirectivesParser(new CommonTokenStream(lexer));

        DirectivesParser.ValueContext context1 = parser.value();
        assertTrue(context1.byteSize() != null);
        assertEquals("10KB", context1.byteSize().getText());

        DirectivesParser.ValueContext context2 = parser.value();
        assertTrue(context2.timeDuration() != null);
        assertEquals("1.5s", context2.timeDuration().getText());
    }
}
