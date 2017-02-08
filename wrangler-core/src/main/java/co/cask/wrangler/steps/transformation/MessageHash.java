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

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A Step to generate a message digest or hash of a column value. .
 */
@Usage(directive = "hash", usage = "hash <column> <algorithm> [replace]")
public class MessageHash extends AbstractStep {
  private static final Map<String, Boolean> algorithms = new TreeMap<>();
  private final String column;
  private final boolean encode;
  private final MessageDigest digest;

  // Initializing available algorithms.
  static {
    algorithms.put("SHA", true);
    algorithms.put("SHA-384", true);
    algorithms.put("SHA-512", true);
    algorithms.put("MD5", true);
    algorithms.put("SHA-256", true);
    algorithms.put("MD2", true);
    algorithms.put("KECCAK-224", true);
    algorithms.put("SHA3-512", true);
    algorithms.put("RIPEMD160", true);
    algorithms.put("KECCAK-512", true);
    algorithms.put("RIPEMD128", true);
    algorithms.put("BLAKE2B-384", true);
    algorithms.put("Skein-512-256", true);
    algorithms.put("WHIRLPOOL", true);
    algorithms.put("Skein-512-224", true);
    algorithms.put("Skein-1024-1024", true);
    algorithms.put("SHA-1", true);
    algorithms.put("SHA-384", true);
    algorithms.put("Skein-512-512", true);
    algorithms.put("GOST3411", true);
    algorithms.put("Skein-1024-512", true);
    algorithms.put("Skein-256-256", true);
    algorithms.put("MD5", true);
    algorithms.put("MD4", true);
    algorithms.put("MD2", true);
    algorithms.put("Skein-256-224", true);
    algorithms.put("SM3", true);
    algorithms.put("Skein-512-160", true);
    algorithms.put("BLAKE2B-256", true);
    algorithms.put("Skein-512-128", true);
    algorithms.put("RIPEMD320", true);
    algorithms.put("GOST3411-2012-256", true);
    algorithms.put("BLAKE2B-512", true);
    algorithms.put("SHA-256", true);
    algorithms.put("SHA-224", true);
    algorithms.put("SHA3-384", true);
    algorithms.put("Skein-256-160", true);
    algorithms.put("Skein-256-128", true);
    algorithms.put("KECCAK-384", true);
    algorithms.put("GOST3411-2012-512", true);
    algorithms.put("TIGER", true);
    algorithms.put("SHA-512", true);
    algorithms.put("SHA-512/256", true);
    algorithms.put("SHA-512/224", true);
    algorithms.put("RIPEMD256", true);
    algorithms.put("BLAKE2B-160", true);
    algorithms.put("Skein-512-384", true);
    algorithms.put("Skein-1024-384", true);
    algorithms.put("Tiger", true);
    algorithms.put("SHA3-256", true);
    algorithms.put("KECCAK-288", true);
    algorithms.put("SHA3-224", true);
    algorithms.put("KECCAK-256", true);
  }

  /**
   * Checks if the algorithm is the one we support.
   *
   * @param algorithm name of the algorithm
   * @return true if we support, false otherwise.
   */
  public static boolean isValid(String algorithm) {
    return (algorithm != null && algorithms.containsKey(algorithm));
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

