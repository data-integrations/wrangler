/*
 * Copyright © 2025 Garv Tayal
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

 package io.cdap.wrangler.grammar;

 import io.cdap.wrangler.grammar.token.Duration;
 import org.junit.Assert;
 import org.junit.Test;
 
 public class DurationTest {
 
     @Test
     public void testDurationParsing() {
         Duration d = new Duration("5s");
         Assert.assertEquals(5000, d.getMilliseconds());
     }
 
     @Test(expected = IllegalArgumentException.class)
     public void testInvalidDuration() {
         new Duration("what?");
     }
 } 

 

