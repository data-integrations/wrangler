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

 package io.cdap.directives.aggregates;

 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.TestingRig;
 import org.junit.Test;
 
 import java.util.Arrays;
 
 /**
  * Tests {@link AggregateStatsDirective}
  */
 public class AggregateStatsDirectiveTest {
     @Test
     public void testAggregateStatsDefaultUnits() throws Exception {
         TestingRig.execute(
             new String[]{"aggregate-stats :size :time total_size_mb total_time_sec"},
             Arrays.asList(
                 new Row("size", "10KB").add("time", "100ms"),
                 new Row("size", "1MB").add("time", "1s")
             )
         );
     }
 
     @Test
     public void testAggregateStatsCustomUnits() throws Exception {
         TestingRig.execute(
             new String[]{"aggregate-stats :size :time total_size_gb total_time_min GB minutes"},
             Arrays.asList(
                 new Row("size", "1MB").add("time", "1s"),
                 new Row("size", "1MB").add("time", "1s")
             )
         );
     }
 
     @Test
     public void testAggregateStatsInvalidInput() throws Exception {
         TestingRig.execute(
             new String[]{"aggregate-stats :size :time total_size_mb total_time_sec"},
             Arrays.asList(
                 new Row("size", "10XB").add("time", "100sec"),
                 new Row("size", "1MB").add("time", "1s")
             )
         );
     }
 }
