/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.directives.aggregates;

import com.google.gson.JsonObject;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.SyntaxError;
import io.cdap.wrangler.api.parser.TimeDuration;

/**
 * Utility class for testing directives.
 */
public final class TestUtils {
  
  private TestUtils() {
    // Prevent instantiation
  }
  
  /**
   * Creates an Arguments instance for testing.
   *
   * @param args Array of arguments in order: sizeColumn, timeColumn, totalSizeColumn, totalTimeColumn
   * @return Arguments instance
   * @throws IllegalArgumentException if args is null or has less than 4 elements
   */
  public static Arguments createArguments(final String... args) {
    if (args == null || args.length < 4) {
      throw new IllegalArgumentException("Required arguments: sizeColumn, timeColumn, totalSizeColumn, totalTimeColumn");
    }
    
    return new Arguments() {
      @SuppressWarnings("unchecked")
      @Override
      public <T> T value(String name) throws DirectiveParseException {
        try {
          switch (name) {
            case "sizeColumn":
              return (T) args[0];
            case "timeColumn":
              return (T) args[1];
            case "totalSizeColumn":
              return (T) args[2];
            case "totalTimeColumn":
              return (T) args[3];
            default:
              throw new DirectiveParseException("Unknown argument: " + name);
          }
        } catch (ArrayIndexOutOfBoundsException e) {
          throw new DirectiveParseException("Missing required argument: " + name);
        } catch (ClassCastException e) {
          throw new DirectiveParseException("Invalid type for argument: " + name);
        }
      }

      @Override
      public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("sizeColumn", args[0]);
        json.addProperty("timeColumn", args[1]);
        json.addProperty("totalSizeColumn", args[2]);
        json.addProperty("totalTimeColumn", args[3]);
        return json;
      }

      @Override
      public String source() {
        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
          if (sb.length() > 0) {
            sb.append(" ");
          }
          sb.append(arg);
        }
        return sb.toString();
      }

      @Override
      public int column() {
        try {
          return Integer.parseInt(args[0]);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
          return 0;
        }
      }
    };
  }

  /**
   * Creates a SyntaxError instance for testing.
   *
   * @param message Error message
   * @return SyntaxError instance
   */
  public static SyntaxError createSyntaxError(String message) {
    return new SyntaxError(1, 1, message, "test input");
  }
} 