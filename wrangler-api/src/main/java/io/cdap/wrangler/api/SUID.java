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

package io.cdap.wrangler.api;

import java.security.SecureRandom;
import java.util.Random;

/**
 * This class <code>SUID</code> creates a unique 64-bit ID with capacity to
 * generate around 64K unique ids within a millisecond.
 */
public final class SUID {
  private static final int SHORT_MAX = 65536;
  private static int counter = -1;

  private SUID() {}

  /**
   * Creates a unique 64 bits ID by aggregating the current time in
   * milliseconds since epoch (Jan. 1, 1970) and using a 16 bits counter. The
   * counter is initialized at a random number. This generator can create up
   * to 65536 different id per millisecond.
   *
   * @return a new id.
   */
  public static synchronized long nextId() {
    if (counter == -1) {
      Random rnd = new SecureRandom();
      counter = rnd.nextInt(SHORT_MAX);
    }
    long now = System.currentTimeMillis();
    long id = (now << 16) | counter;
    counter = (counter + 1) % SHORT_MAX;
    return id;
  }
}
