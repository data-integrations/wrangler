/*
 * Copyright © 2016-2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.cdap.wrangler.parser.WranglerParser.ByteSizeArgContext;
import io.cdap.wrangler.parser.WranglerParser.TimeDurationArgContext;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RecipeVisitorTest {

    @Test
    public void testVisitByteSizeArg() {
        // Mock the parser context
        ByteSizeArgContext ctx = mock(ByteSizeArgContext.class);
        when(ctx.getText()).thenReturn("10MB");

        RecipeVisitor visitor = new RecipeVisitor();
        Object result = visitor.visitByteSizeArg(ctx);

        assertTrue(result instanceof ByteSize);
        assertEquals("10MB", result.toString()); // Or compare based on actual ByteSize implementation
    }

    @Test
    public void testVisitTimeDurationArg() {
        // Mock the parser context
        TimeDurationArgContext ctx = mock(TimeDurationArgContext.class);
        when(ctx.getText()).thenReturn("200ms");

        RecipeVisitor visitor = new RecipeVisitor();
        Object result = visitor.visitTimeDurationArg(ctx);

        assertTrue(result instanceof TimeDuration);
        assertEquals("200ms", result.toString()); // Or compare based on actual TimeDuration implementation
    }
}
