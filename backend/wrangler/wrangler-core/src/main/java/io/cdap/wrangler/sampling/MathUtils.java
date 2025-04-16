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

package io.cdap.wrangler.sampling;

/**
 * Collection of simple mathematical routines.
 */
public final class MathUtils {

  /**
   * This function hashes an integer value.
   *
   * It is crucial to use different hash functions to partition data and the internal partitioning of
   * data structures. This hash function is intended for partitioning across machines.
   *
   * @param code The integer to be hashed.
   * @return The non-negative hash code for the integer.
   */
  public static int murmurHash(int code) {
    code *= 0xcc9e2d51;
    code = Integer.rotateLeft(code, 15);
    code *= 0x1b873593;

    code = Integer.rotateLeft(code, 13);
    code = code * 5 + 0xe6546b64;

    code ^= 4;
    code = bitMix(code);

    if (code >= 0) {
      return code;
    } else if (code != Integer.MIN_VALUE) {
      return -code;
    } else {
      return 0;
    }
  }

  /**
   * Bit-mixing for pseudo-randomization of integers (e.g., to guard against bad hash functions). Implementation is
   * from Murmur's 32 bit finalizer.
   *
   * @param in the input value
   * @return the bit-mixed output value
   */
  public static int bitMix(int in) {
    in ^= in >>> 16;
    in *= 0x85ebca6b;
    in ^= in >>> 13;
    in *= 0xc2b2ae35;
    in ^= in >>> 16;
    return in;
  }

  /**
   * Prevent Instantiation through private constructor.
   */
  private MathUtils() {}
}
