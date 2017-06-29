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

package co.cask.wrangler.parser;

import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.CompiledUnit;
import co.cask.wrangler.api.Compiler;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.TokenGroup;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * Tests {@link MapArguments}.
 */
public class MapArgumentsTest {

  @Test
  public void testWithAllRequiredFields() throws Exception {
    Compiler compiler = new RecipeCompiler();
    CompiledUnit unit = compiler.compile("rename :fname :lname;");

    UsageDefinition.Builder builder = UsageDefinition.builder("rename");
    builder.define("col1", TokenType.COLUMN_NAME);
    builder.define("col2", TokenType.COLUMN_NAME);

    Iterator<TokenGroup> iterator = unit.iterator();
    Arguments arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(2, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(true, arguments.contains("col2"));
  }

  @Test
  public void testWithOptionalField() throws Exception {
    Compiler compiler = new RecipeCompiler();
    CompiledUnit unit = compiler.compile("rename :fname;");

    UsageDefinition.Builder builder = UsageDefinition.builder("rename");
    builder.define("col1", TokenType.COLUMN_NAME);
    builder.define("col2", TokenType.COLUMN_NAME, Optional.TRUE);

    Iterator<TokenGroup> iterator = unit.iterator();
    Arguments arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(1, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(false, arguments.contains("col2"));
  }

  @Test
  public void testMultipleArgumentsOptional() throws Exception {
    Compiler compiler = new RecipeCompiler();
    CompiledUnit unit1 = compiler.compile("parse-as-csv :body ' ';");
    CompiledUnit unit2 = compiler.compile("parse-as-csv :body ' ' true;");
    CompiledUnit unit3 = compiler.compile("parse-as-csv :body ' ' true exp: { type == '002' };");
    CompiledUnit unit4 = compiler.compile("parse-as-csv :body exp: { type == '002' };");

    UsageDefinition.Builder builder = UsageDefinition.builder("rename");
    builder.define("col1", TokenType.COLUMN_NAME);
    builder.define("col2", TokenType.TEXT, Optional.TRUE);
    builder.define("col3", TokenType.BOOLEAN, Optional.TRUE);
    builder.define("col4", TokenType.EXPRESSION, Optional.TRUE);

    Iterator<TokenGroup> iterator = unit1.iterator();
    Arguments arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(2, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(true, arguments.contains("col2"));
    Assert.assertEquals(false, arguments.contains("col3"));

    iterator = unit2.iterator();
    arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(3, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(true, arguments.contains("col2"));
    Assert.assertEquals(true, arguments.contains("col3"));

    iterator = unit3.iterator();
    arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(4, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(true, arguments.contains("col2"));
    Assert.assertEquals(true, arguments.contains("col3"));
    Assert.assertEquals(true, arguments.contains("col4"));

    iterator = unit4.iterator();
    arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(2, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(false, arguments.contains("col2"));
    Assert.assertEquals(false, arguments.contains("col3"));
    Assert.assertEquals(true, arguments.contains("col4"));
  }

}