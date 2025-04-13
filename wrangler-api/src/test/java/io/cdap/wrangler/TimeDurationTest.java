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

 package io.cdap.wrangler;

 import org.junit.Assert;
 import org.junit.Test;

import io.cdap.wrangler.api.parser.TimeDuration;
 
 public class TimeDurationTest {
 
     @Test
     public void testValidDurations() {
         Assert.assertEquals(15L, new TimeDuration("15ms").getMilliseconds());
         Assert.assertEquals(2200L, new TimeDuration("2.2s").getMilliseconds());
         Assert.assertEquals(120000L, new TimeDuration("2min").getMilliseconds());
     }
 
     @Test(expected = IllegalArgumentException.class)
     public void testInvalidTimeDuration() {
         new TimeDuration("7lightyears"); // nonsense unit
     }
 }
 
 