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

package co.cask.wrangler.internal.sampling;

import com.google.common.base.Predicate;

import java.util.Random;
import javax.annotation.Nullable;

/**
 * A stateful predicate that, given a total number
 * of items and the number to choose, will return 'true'
 * the chosen number of times distributed randomly
 * across the total number of calls to its test() method.
 */
public class RandomDistributionSampling implements Predicate<Object> {
  private int total;  // total number items remaining
  private int remain; // number of items remaining to select
  private Random random = new Random();

  public RandomDistributionSampling(int total, int remain) {
    this.total = total;
    this.remain = remain;
  }

  @Override
  public boolean apply(@Nullable Object o) {
    if (random.nextInt(total--) < remain) {
      remain--;
      return true;
    } else {
      return false;
    }
  }
}
