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

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * A simple in memory implementation of Reservoir Sampling without replacement, and with only one
 * pass through the input iteration whose size is unpredictable. The basic idea behind this sampler
 * implementation is to generate a random number for each input element as its weight, select the
 * top K elements with max weight. As the weights are generated randomly, so are the selected
 * top K elements. In the first phase, we generate random numbers as the weights for each element and
 * select top K elements as the output of each partitions.
 *
 * @param <T> The type of the sampler.
 */
public class Reservoir<T> extends Sampler<T> {
  private final int numSamples;
  private final Random random;

  /**
   * Create a new sampler with reservoir size and a supplied random number generator.
   *
   * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
   * @param random     Instance of random number generator for sampling.
   */
  public Reservoir(int numSamples, Random random) {
    Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
    this.numSamples = numSamples;
    this.random = random;
  }

  /**
   * Create a new sampler with reservoir size and a default random number generator.
   *
   * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
   */
  public Reservoir(int numSamples) {
    this(numSamples, new XORShiftRNG());
  }

  /**
   * Create a new sampler with reservoir size and the seed for random number generator.
   *
   * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
   * @param seed       Random number generator seed.
   */
  public Reservoir(int numSamples, long seed) {
    this(numSamples, new XORShiftRNG(seed));
  }

  @Override
  public Iterator<T> sample(Iterator<T> input) {
    if (numSamples == 0) {
      return emptyIterable;
    }

    // This queue holds fixed number elements with the top K weight for current partition.
    PriorityQueue<IntermediateSample<T>> queue = new PriorityQueue<>(numSamples);
    int index = 0;
    IntermediateSample<T> smallest = null;
    while (input.hasNext()) {
      T element = input.next();
      if (index < numSamples) {
        // Fill the queue with first K elements from input.
        queue.add(new IntermediateSample<T>(random.nextDouble(), element));
        smallest = queue.peek();
      } else {
        double rand = random.nextDouble();
        // Remove the element with the smallest weight, and append current element into the queue.
        if (rand > smallest.getWeight()) {
          queue.remove();
          queue.add(new IntermediateSample<T>(rand, element));
          smallest = queue.peek();
        }
      }
      index++;
    }

    final Iterator<IntermediateSample<T>> itr = queue.iterator();
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return itr.hasNext();
      }

      @Override
      public T next() {
        return itr.next().getElement();
      }

      @Override
      public void remove() {
        itr.remove();
      }
    };
  }

  /**
   * An intermediate sample
   *
   * @param <T> the element type
   */
  public static class IntermediateSample<T> implements Comparable<IntermediateSample<T>> {
    private double weight;
    private T element;

    public IntermediateSample(double weight, T element) {
      this.weight = weight;
      this.element = element;
    }

    public double getWeight() {
      return weight;
    }

    public T getElement() {
      return element;
    }

    @Override
    public int compareTo(IntermediateSample<T> other) {
      return this.weight >= other.getWeight() ? 1 : -1;
    }
  }
}
