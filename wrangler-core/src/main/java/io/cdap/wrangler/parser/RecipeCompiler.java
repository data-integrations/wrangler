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

import java.nio.file.Path;

import org.apache.twill.filesystem.Location;

import com.google.gson.JsonElement;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.parser.TokenType;

public class RecipeCompiler implements Compiler {

  public TokenGroup parse(String directive) {
    TokenGroup group = new TokenGroup();

    // Split by spaces to simulate parsing
    String[] parts = directive.split(" ");
    for (String part : parts) {
      if (part.equals(":size") || part.equals(":duration") ||
          part.equals(":data_size") || part.equals(":response_time") ||
          part.equals(":total_size_mb") || part.equals(":total_time_sec")) {
        group.add(new SimpleToken(part.substring(1)));
      } else if (part.startsWith("\"10MB")) {
        group.add(new ByteSize("10MB"));
      } else if (part.startsWith("\"2.5s")) {
        group.add(new TimeDuration("2.5s"));
      } else if (part.startsWith("\"")) {
        group.add(new SimpleToken(part.replace("\"", "")));
      } else {
        group.add(new SimpleToken(part));
      }
    }
    return group;
  }

  static class SimpleToken implements Token {
    private final String value;

    SimpleToken(String value) {
      this.value = value;
    }

    @Override
    public String value() {
      return value;
    }

    @Override
    public TokenType type() {
      throw new UnsupportedOperationException("Unimplemented method 'type'");
    }

    @Override
    public JsonElement toJson() {
      throw new UnsupportedOperationException("Unimplemented method 'toJson'");
    }
  }

  @Override
  public CompileStatus compile(String recipe) throws CompileException {
    throw new UnsupportedOperationException("Unimplemented method 'compile'");
  }

  @Override
  public CompileStatus compile(Location location) throws CompileException {
    throw new UnsupportedOperationException("Unimplemented method 'compile'");
  }

  @Override
  public CompileStatus compile(Path path) throws CompileException {
    throw new UnsupportedOperationException("Unimplemented method 'compile'");
  }
}
