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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TextList;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

/**
 * Tests {@link MapArguments}.
 */
public class MapArgumentsTest {

  @Test
  public void testWithAllRequiredFields() throws Exception {
    Compiler compiler = new RecipeCompiler();
    CompileStatus status = compiler.compile("rename :fname :lname;");

    UsageDefinition.Builder builder = UsageDefinition.builder("rename");
    builder.define("col1", TokenType.COLUMN_NAME);
    builder.define("col2", TokenType.COLUMN_NAME);

    Iterator<TokenGroup> iterator = status.getSymbols().iterator();
    Arguments arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(2, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(true, arguments.contains("col2"));
  }

  @Test
  public void testWithOptionalField() throws Exception {
    Compiler compiler = new RecipeCompiler();
    CompileStatus status = compiler.compile("rename :fname;");

    UsageDefinition.Builder builder = UsageDefinition.builder("rename");
    builder.define("col1", TokenType.COLUMN_NAME);
    builder.define("col2", TokenType.COLUMN_NAME, Optional.TRUE);

    Iterator<TokenGroup> iterator = status.getSymbols().iterator();
    Arguments arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(1, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(false, arguments.contains("col2"));
  }

  @Test
  public void testMultipleArgumentsOptional() throws Exception {
    Compiler compiler = new RecipeCompiler();
    CompileStatus status1 = compiler.compile("parse-as-csv :body ' ';");
    CompileStatus status2 = compiler.compile("parse-as-csv :body ' ' true;");
    CompileStatus status3 = compiler.compile("parse-as-csv :body ' ' true exp: { type == '002' };");
    CompileStatus status4 = compiler.compile("parse-as-csv :body exp: { type == '002' };");

    UsageDefinition.Builder builder = UsageDefinition.builder("rename");
    builder.define("col1", TokenType.COLUMN_NAME);
    builder.define("col2", TokenType.TEXT, Optional.TRUE);
    builder.define("col3", TokenType.BOOLEAN, Optional.TRUE);
    builder.define("col4", TokenType.EXPRESSION, Optional.TRUE);

    Iterator<TokenGroup> iterator = status1.getSymbols().iterator();
    Arguments arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(2, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(true, arguments.contains("col2"));
    Assert.assertEquals(false, arguments.contains("col3"));

    iterator = status2.getSymbols().iterator();
    arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(3, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(true, arguments.contains("col2"));
    Assert.assertEquals(true, arguments.contains("col3"));

    iterator = status3.getSymbols().iterator();
    arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(4, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(true, arguments.contains("col2"));
    Assert.assertEquals(true, arguments.contains("col3"));
    Assert.assertEquals(true, arguments.contains("col4"));

    iterator = status4.getSymbols().iterator();
    arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(2, arguments.size());
    Assert.assertEquals(true, arguments.contains("col1"));
    Assert.assertEquals(false, arguments.contains("col2"));
    Assert.assertEquals(false, arguments.contains("col3"));
    Assert.assertEquals(true, arguments.contains("col4"));
  }

  @Test
  public void testTextLists() throws Exception {
    Compiler compiler = new RecipeCompiler();
    CompileStatus status = compiler.compile("remove-sensitive-data :body \"ALL_BASIC\", \"AGE\";");

    UsageDefinition.Builder builder = UsageDefinition.builder("rename");
    builder.define("body", TokenType.COLUMN_NAME);
    builder.define("infoTypes", TokenType.TEXT_LIST);

    Iterator<TokenGroup> iterator = status.getSymbols().iterator();
    Arguments arguments = new MapArguments(builder.build(), iterator.next());
    Assert.assertEquals(2, arguments.size());
    Assert.assertTrue(arguments.contains("body"));
    Assert.assertTrue(arguments.contains("infoTypes"));
    ColumnName columnName = arguments.value("body");
    Assert.assertEquals("body", columnName.value());
    List<String> infoTypes = ((TextList) arguments.value("infoTypes")).value();
    Assert.assertEquals(2, infoTypes.size());
    Assert.assertEquals("ALL_BASIC", infoTypes.get(0));
    Assert.assertEquals("AGE", infoTypes.get(1));
  }

}
