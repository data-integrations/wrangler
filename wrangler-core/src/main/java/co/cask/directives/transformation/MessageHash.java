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
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.Bool;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.common.collect.ImmutableSet;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;

/**
 * A Executor to generate a message digest or hash of a column value. .
 */
@Plugin(type = Directive.Type)
@Name(MessageHash.NAME)
@Categories(categories = { "transform", "hash"})
@Description("Creates a message digest for the column using algorithm, replacing the column value.")
public class MessageHash implements Directive {
  public static final String NAME = "hash";
  private static final Set<String> algorithms = ImmutableSet.of(
    "BLAKE2B-160",
    "BLAKE2B-256",
    "BLAKE2B-384",
    "BLAKE2B-512",
    "GOST3411-2012-256",
    "GOST3411-2012-512",
    "GOST3411",
    "KECCAK-224",
    "KECCAK-256",
    "KECCAK-288",
    "KECCAK-384",
    "KECCAK-512",
    "MD2",
    "MD2",
    "MD4",
    "MD5",
    "RIPEMD128",
    "RIPEMD160",
    "RIPEMD256",
    "RIPEMD320",
    "SHA-1",
    "SHA-224",
    "SHA-256",
    "SHA-384",
    "SHA-512",
    "SHA-512/224",
    "SHA-512/256",
    "SHA",
    "SHA3-224",
    "SHA3-256",
    "SHA3-384",
    "SHA3-512",
    "Skein-1024-1024",
    "Skein-1024-384",
    "Skein-1024-512",
    "Skein-256-128",
    "Skein-256-160",
    "Skein-256-224",
    "Skein-256-256",
    "Skein-512-128",
    "Skein-512-160",
    "Skein-512-224",
    "Skein-512-256",
    "Skein-512-384",
    "Skein-512-512",
    "SM3",
    "Tiger",
    "TIGER",
    "WHIRLPOOL"
  );
  private String column;
  private boolean encode;
  private MessageDigest digest;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("algorithm", TokenType.TEXT);
    builder.define("encode", TokenType.BOOLEAN, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    Text algorithm = args.value("algorithm");
    if (!MessageHash.isValid(algorithm.value())) {
      throw new DirectiveParseException(
        String.format("Algorithm '%s' specified in directive '%s' at line %d is not supported", algorithm,
                      NAME, args.line())
      );
    }
    try {
      this.digest = MessageDigest.getInstance(algorithm.value());
    } catch (NoSuchAlgorithmException e) {
      throw new DirectiveParseException(
        String.format("Unable to find algorithm specified '%s' in directive '%s' at line %d.",
                      algorithm, NAME, args.line())
      );
    }

    this.encode = false;
    if(args.contains("encode")) {
      this.encode = ((Bool) args.value("encode")).value();
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  public static boolean isValid(String algorithm) {
    return (algorithm != null && algorithms.contains(algorithm));
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        byte[] message;
        if (object instanceof String) {
          message = ((String) object).getBytes(StandardCharsets.UTF_8);
        } else if (object instanceof byte[]) {
          message = ((byte[]) object);
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String or byte[].", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );
        }

        digest.update(message);
        byte[] hashed = digest.digest();

        if (encode) {
          // hex with left zero padding:
          String hasedHex = String.format("%064x", new java.math.BigInteger(1, hashed));
          row.addOrSet(column, hasedHex);
        } else {
          row.addOrSet(column, hashed);
        }
      } else {
        throw new DirectiveExecutionException(toString() + " : Column '" + column + "' does not exist in the row.");
      }
    }
    return rows;
  }
}

