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

public class ByteSize extends Token {
    private final double valueInBytes;

    public ByteSize(String value) {
        super(value);
        this.valueInBytes = parseBytes(value.trim().toUpperCase());
    }

    private double parseBytes(String str) {
        double multiplier;
        if (str.endsWith("KB")) {
            multiplier = 1024;
            str = str.replace("KB", "");
        } else if (str.endsWith("MB")) {
            multiplier = 1024 * 1024;
            str = str.replace("MB", "");
        } else if (str.endsWith("GB")) {
            multiplier = 1024 * 1024 * 1024;
            str = str.replace("GB", "");
        } else if (str.endsWith("B")) {
            multiplier = 1;
            str = str.replace("B", "");
        } else {
            throw new IllegalArgumentException("Invalid byte size: " + str);
        }
        return Double.parseDouble(str) * multiplier;
    }

    public long getBytes() {
        return (long) valueInBytes;
    }
}

public class TimeDuration extends Token {
    private final double valueInMillis;

    public TimeDuration(String value) {
        super(value);
        this.valueInMillis = parseMillis(value.trim().toLowerCase());
    }

    private double parseMillis(String str) {
        double multiplier;
        if (str.endsWith("ms")) {
            multiplier = 1;
            str = str.replace("ms", "");
        } else if (str.endsWith("s")) {
            multiplier = 1000;
            str = str.replace("s", "");
        } else if (str.endsWith("m")) {
            multiplier = 60 * 1000;
            str = str.replace("m", "");
        } else if (str.endsWith("h")) {
            multiplier = 60 * 60 * 1000;
            str = str.replace("h", "");
        } else {
            throw new IllegalArgumentException("Invalid time duration: " + str);
        }
        return Double.parseDouble(str) * multiplier;
    }

    public long getMillis() {
        return (long) valueInMillis;
    }
}


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

    Assert.assertTrue(true);
  }

}
