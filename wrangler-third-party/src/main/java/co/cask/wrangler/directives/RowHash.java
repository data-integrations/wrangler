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

package co.cask.wrangler.directives;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Identifier;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.List;

/**
 * This directive generates a the row hash using all the column values of a row.
 */
@Plugin(type = Directive.Type)
@Name(RowHash.NAME)
@Description("Generates the row hash using different hashing techniques.")
public final class RowHash implements Directive {
  public static final String NAME = "row-hash";
  private String column;
  private String codec;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("codec", TokenType.IDENTIFIER);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.codec = ((Identifier) args.value("codec")).value();
    if(!codec.equalsIgnoreCase("md5") && !codec.equalsIgnoreCase("sha1") &&
       !codec.equalsIgnoreCase("sha256") && !codec.equalsIgnoreCase("sha384") &&
       !codec.equalsIgnoreCase("sha512")) {
      throw new DirectiveParseException(
        String.format("Invalid code '%s' specified. Allowed only md5,sha1,sha256,sha384,sha512", codec)
      );
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException, ErrorRowException {
    for (Row row : rows) {
      switch (codec) {
        case "md5": {
          String md5 = DigestUtils.md5Hex(row.toString()).toUpperCase();
          row.addOrSet(column, md5);
        }
        break;

        case "sha1": {
          String sha1 = DigestUtils.sha1Hex(row.toString()).toUpperCase();
          row.addOrSet(column, sha1);
        }
        break;

        case "sha256": {
          String sha256 = DigestUtils.sha256Hex(row.toString()).toUpperCase();
          row.addOrSet(column, sha256);
        }
        break;

        case "sha384": {
          String sha384 = DigestUtils.sha384Hex(row.toString()).toUpperCase();
          row.addOrSet(column, sha384);
        }
        break;

        case "sha512": {
          String sha512 = DigestUtils.sha512Hex(row.toString()).toUpperCase();
          row.addOrSet(column, sha512);
        }
        break;
      }
    }
    return rows;
  }
}
