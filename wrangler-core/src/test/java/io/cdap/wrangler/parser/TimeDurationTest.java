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

 package io.cdap.wrangler.parser;

 import io.cdap.wrangler.api.parser.TimeDuration;
 import org.junit.Test;
 
 /**
  * Tests {@link TimeDuration}
  */
 public class TimeDurationTest {
     @Test
     public void testValidTimeDurationMS() throws Exception {
         new TimeDuration("100ms");
     }
 
     @Test
     public void testValidTimeDurationSeconds() throws Exception {
         new TimeDuration("2s");
     }
 
     @Test
     public void testValidTimeDurationMinutes() throws Exception {
         new TimeDuration("1.5m");
     }
 
     @Test(expected = IllegalArgumentException.class)
     public void testInvalidTimeDurationUnit() throws Exception {
         new TimeDuration("10sec");
     }
 
     @Test(expected = IllegalArgumentException.class)
     public void testInvalidTimeDurationFormat() throws Exception {
         new TimeDuration("10");
     }
 }