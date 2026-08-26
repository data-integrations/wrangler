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
import io.cdap.wrangler.api.TokenGroup;
import org.junit.Assert;
import org.junit.Test;

public class DirectivesParserTest {

@Test
public void testByteSizeTokenParsing() throws Exception {
String directive = "set-column :size \"10MB\"";
RecipeCompiler compiler = new RecipeCompiler();
TokenGroup tokenGroup = compiler.parse(directive);

// Verify tokens are correctly parsed
Token token = tokenGroup.get(3); // The fourth token should be the byte size
Assert.assertTrue(token instanceof ByteSize);
ByteSize byteSize = (ByteSize) token;
Assert.assertEquals(10 * 1024 * 1024, byteSize.getBytes());
}

@Test
public void testTimeDurationTokenParsing() throws Exception {
String directive = "set-column :duration \"2.5s\"";
RecipeCompiler compiler = new RecipeCompiler();
TokenGroup tokenGroup = compiler.parse(directive);

// Verify tokens are correctly parsed
Token token = tokenGroup.get(3); // The fourth token should be the time duration
Assert.assertTrue(token instanceof TimeDuration);
TimeDuration timeDuration = (TimeDuration) token;
Assert.assertEquals(2.5 * 1_000_000_000, timeDuration.getNanoseconds(), 0.5);
}

@Test
public void testAggregateStatsDirectiveParsing() throws Exception {
String directive = "aggregate-stats :data_size :response_time :total_size_mb :total_time_sec";
RecipeCompiler compiler = new RecipeCompiler();
TokenGroup tokenGroup = compiler.parse(directive);

// Verify directive name and column names are parsed correctly
Assert.assertEquals("aggregate-stats", tokenGroup.get(0).value());
Assert.assertEquals("data_size", tokenGroup.get(1).value());
Assert.assertEquals("response_time", tokenGroup.get(2).value());
Assert.assertEquals("total_size_mb", tokenGroup.get(3).value());
Assert.assertEquals("total_time_sec", tokenGroup.get(4).value());
}
}