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

import co.cask.wrangler.api.AbstractIndependentStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableSet;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;

/**
 * A Step to generate a message digest or hash of a column value. .
 */
@Usage(
  directive = "hash",
  usage = "hash <column> <algorithm> [encode]",
  description = "Creates a message digest for the column."
)
public class MessageHash extends AbstractIndependentStep {
  private static final Set<String> algorithms = ImmutableSet.of(
    "SHA",
    "SHA-384",
    "SHA-512",
    "MD5",
    "SHA-256",
    "MD2",
    "KECCAK-224",
    "SHA3-512",
    "RIPEMD160",
    "KECCAK-512",
    "RIPEMD128",
    "BLAKE2B-384",
    "Skein-512-256",
    "WHIRLPOOL",
    "Skein-512-224",
    "Skein-1024-1024",
    "SHA-1",
    "SHA-384",
    "Skein-512-512",
    "GOST3411",
    "Skein-1024-512",
    "Skein-256-256",
    "MD5",
    "MD4",
    "MD2",
    "Skein-256-224",
    "SM3",
    "Skein-512-160",
    "BLAKE2B-256",
    "Skein-512-128",
    "RIPEMD320",
    "GOST3411-2012-256",
    "BLAKE2B-512",
    "SHA-256",
    "SHA-224",
    "SHA3-384",
    "Skein-256-160",
    "Skein-256-128",
    "KECCAK-384",
    "GOST3411-2012-512",
    "TIGER",
    "SHA-512",
    "SHA-512/256",
    "SHA-512/224",
    "RIPEMD256",
    "BLAKE2B-160",
    "Skein-512-384",
    "Skein-1024-384",
    "Tiger",
    "SHA3-256",
    "KECCAK-288",
    "SHA3-224",
    "KECCAK-256"
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
    super(lineno, directive, column);
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

