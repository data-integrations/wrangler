/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableSet;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;

/**
 * A Directive to generate a message digest or hash of a column value. .
 */
@Plugin(type = "udd")
@Name("hash")
@Usage("hash <column> <algorithm> [<encode=true|false>]")
@Description("Creates a message digest for the column using algorithm, replacing the column value.")
public class MessageHash extends AbstractStep {
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
  private final String column;
  private final boolean encode;
  private final MessageDigest digest;

  /**
   * Checks if the algorithm is the one we support.
   *
   * @param algorithm name of the algorithm
   * @return true if we support, false otherwise.
   */
  public static boolean isValid(String algorithm) {
    return (algorithm != null && algorithms.contains(algorithm));
  }

  public MessageHash(int lineno, String directive, String column, MessageDigest digest, boolean encode) {
    super(lineno, directive);
    this.column = column;
    this.digest = digest;
    this.encode = encode;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        byte[] message;
        if (object instanceof String) {
          message = ((String) object).getBytes(StandardCharsets.UTF_8);
        } else if (object instanceof byte[]) {
          message = ((byte[]) object);
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String or byte[].", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );
        }

        digest.update(message);
        byte[] hashed = digest.digest();

        if (encode) {
          // hex with left zero padding:
          String hasedHex = String.format("%064x", new java.math.BigInteger(1, hashed));
          record.addOrSet(column, hasedHex);
        } else {
          record.addOrSet(column, hashed);
        }
      } else {
        throw new StepException(toString() + " : Column '" + column + "' does not exist in the record.");
      }
    }
    return records;
  }
}

