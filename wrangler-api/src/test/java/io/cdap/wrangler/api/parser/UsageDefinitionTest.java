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

package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.api.Optional;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link UsageDefinition}
 */
public class UsageDefinitionTest {

  @Test
  public void testUsageDefinitionCreation() {
    UsageDefinition.Builder builder = UsageDefinition.builder("test");
    builder.define("fname", TokenType.COLUMN_NAME);
    builder.define("lname", TokenType.COLUMN_NAME, Optional.TRUE);
    builder.define("condition", TokenType.EXPRESSION, Optional.TRUE);
    UsageDefinition definition = builder.build();
    Assert.assertEquals("test", definition.getDirectiveName());
  }

  @Test
  public void testUsageStringCreation() {
    List<String> usages = new ArrayList<>();

    UsageDefinition.Builder builder = UsageDefinition.builder("rename");
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    String usage = builder.build().toString();
    Assert.assertEquals("rename :source :destination", usage);
    usages.add(usage);

    builder = UsageDefinition.builder("parse-as-csv");
    builder.define("col", TokenType.COLUMN_NAME);
    builder.define("delimiter", TokenType.TEXT, Optional.TRUE);
    builder.define("header", TokenType.BOOLEAN, Optional.TRUE);
    usage = builder.build().toString();
    Assert.assertEquals("parse-as-csv :col  ['delimiter'] [header (true/false)]", usage);
    usages.add(usage);

    builder = UsageDefinition.builder("send-to-error");
    builder.define("expr", TokenType.EXPRESSION);
    builder.define("metric", TokenType.TEXT, Optional.TRUE);
    usage = builder.build().toString();
    Assert.assertEquals("send-to-error exp:{<expr>}  ['metric']", usage);
    usages.add(usage);

    builder = UsageDefinition.builder("set-columns");
    builder.define("cols", TokenType.COLUMN_NAME_LIST);
    usage = builder.build().toString();
    Assert.assertEquals("set-columns :cols [,:cols  ]*", usage);
    usages.add(usage);
    public class ByteSize implements Token {
    private final long bytes;

    public ByteSize(String input) {
        input = input.toUpperCase().trim();
        if (input.endsWith("KB")) bytes = (long)(Double.parseDouble(input.replace("KB", "")) * 1024);
        else if (input.endsWith("MB")) bytes = (long)(Double.parseDouble(input.replace("MB", "")) * 1024 * 1024);
        else if (input.endsWith("GB")) bytes = (long)(Double.parseDouble(input.replace("GB", "")) * 1024 * 1024 * 1024);
        else if (input.endsWith("TB")) bytes = (long)(Double.parseDouble(input.replace("TB", "")) * 1024L * 1024 * 1024 * 1024);
        else if (input.endsWith("B")) bytes = Long.parseLong(input.replace("B", ""));
        else throw new IllegalArgumentException("Invalid byte size format: " + input);
    }

    public long getBytes() {
        return bytes;
    }
}
public class TimeDuration implements Token {
    private final long millis;

    public TimeDuration(String input) {
        input = input.trim();
        if (input.endsWith("ms")) millis = Long.parseLong(input.replace("ms", ""));
        else if (input.endsWith("s")) millis = Long.parseLong(input.replace("s", "")) * 1000;
        else if (input.endsWith("m")) millis = Long.parseLong(input.replace("m", "")) * 60 * 1000;
        else if (input.endsWith("h")) millis = Long.parseLong(input.replace("h", "")) * 3600 * 1000;
        else throw new IllegalArgumentException("Invalid time format: " + input);
    }

    public long getMillis() {
        return millis;
    }
}


    Assert.assertTrue(true);
  }

}
