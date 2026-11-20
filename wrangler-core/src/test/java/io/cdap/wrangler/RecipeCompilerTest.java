//        Copyright © 2018-2019 Cask Data, Inc.
//
//        Licensed under the Apache License, Version 2.0 (the "License"); you may not
//        use this file except in compliance with the License. You may obtain a copy of
//        the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//        Unless required by applicable law or agreed to in writing, software
//        distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//        WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//        License for the specific language governing permissions and limitations under
//        the License.

package io.cdap.wrangler;

import io.cdap.wrangler.api.ByteSize;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RecipeCompilerTest {
    @Test
    public void testAggregateStatsTotal() throws Exception {
        List<Row> input = List.of(
                new Row("data_transfer_size", new ByteSize("10MB"))
                        .add("response_time", new TimeDuration("2s")),
                new Row("data_transfer_size", new ByteSize("5MB"))
                        .add("response_time", new TimeDuration("1s"))
        );

        String[] recipe = {
                "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        List<Row> output = TestingRig.execute(recipe, input);

        Assert.assertEquals(1, output.size());
        Row result = output.get(0);

        Assert.assertEquals(15.0, result.getValue("total_size_mb")); // 10MB + 5MB
        Assert.assertEquals(3.0, result.getValue("total_time_sec")); // 2s + 1s
    }

}
