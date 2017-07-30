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

package co.cask.directives.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * A directive that decodes a column that was encoded as base-32, base-64, or hex.
 */
@Plugin(type = Directive.Type)
@Name(Decode.NAME)
@Categories(categories = { "transform"})
@Description("Decodes column values using one of base32, base64, or hex.")
public class Decode implements Directive {
  public static final String NAME = "decode";
  private final Base64 base64Encode = new Base64();
  private final Base32 base32Encode = new Base32();
  private final Hex hexEncode = new Hex();
  private Method method;
  private String column;

  /**
   * Defines encoding types supported.
   */
  public enum Method {
    BASE64("BASE64"),
    BASE32("BASE32"),
    HEX("HEX");

    private String type;

    Method(String type) {
      this.type = type;
    }

    String getType() {
      return type;
    }
  }

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("method", TokenType.TEXT);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    String type = ((Text) args.value("method")).value();
    type = type.toUpperCase();
    if (!type.equals("BASE64") && !type.equals("BASE32") && !type.equals("HEX")) {
      throw new DirectiveParseException(
        String.format("Type of decoding specified '%s' is not supported. Supports base64, base32 & hex.",
                      type)
      );
    }
    this.method = Method.valueOf(type);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        continue;
      }

      Object object = row.getValue(idx);
      if (object == null) {
        continue;
      }

      byte[] value = new byte[0];
      if (object instanceof String) {
        value = ((String) object).getBytes();
      } else if (object instanceof byte[]) {
        value = (byte[]) object;
      } else {
        throw new DirectiveExecutionException(
          String.format("%s : Invalid value type '%s' of column '%s'. Should be of type string or byte array, "
            , toString(), value.getClass().getName(), column)
        );
      }

      byte[] out = new byte[0];
      if (method == Method.BASE32) {
        out = base32Encode.decode(value);
      } else if (method == Method.BASE64) {
        out = base64Encode.decode(value);
      } else if (method == Method.HEX) {
        try {
          out = hexEncode.decode(value);
        } catch (DecoderException e) {
          throw new DirectiveExecutionException(
            String.format("%s : Failed to decode hex value.", toString())
          );
        }
      } else {
        throw new DirectiveExecutionException(
          String.format("%s : Invalid type of encoding '%s' specified", toString(), method.toString())
        );
      }

      String obj = new String(out, StandardCharsets.UTF_8);
      row.addOrSet(String.format("%s_decode_%s", column, method.toString().toLowerCase(Locale.ENGLISH)), obj);
    }
    return rows;
  }
}
