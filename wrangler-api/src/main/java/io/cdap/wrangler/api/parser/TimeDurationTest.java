/*
 * Copyright © [2025] [Nitin]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cdap.wrangler.api.parser;

public class TimeDurationTest {
    public static void main(String[] args) {
        TimeDuration t1 = new TimeDuration("500ms");
        TimeDuration t2 = new TimeDuration("2s");
        TimeDuration t3 = new TimeDuration("1.5min");
        TimeDuration t4 = new TimeDuration("3h");
        TimeDuration t5 = new TimeDuration("1d");

        System.out.println("T1: " + t1.toMilliseconds() + " ms");
        System.out.println("T2: " + t2.toMilliseconds() + " ms");
        System.out.println("T3: " + t3.toMilliseconds() + " ms");
        System.out.println("T4: " + t4.toMilliseconds() + " ms");
        System.out.println("T5: " + t5.toMilliseconds() + " ms");
    }
}
