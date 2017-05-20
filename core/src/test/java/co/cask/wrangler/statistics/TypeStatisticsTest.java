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

package co.cask.wrangler.statistics;

import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.statistics.Statistics;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kewang on 5/19/17.
 */
public class TypeStatisticsTest {
  @Test
  public void testBasicStatistics() throws Exception {

    Record record1 = new Record ("phone", "217-418-5708");
    record1.add("address", "lincoln");

    Record record2 = new Record ("phone", "217-333-1303");
    record2.add("address", "john");

    Record record3 = new Record ("phone", "universe");
    record3.add("address", "217-418-5708");

    Record record4 = new Record ("phone", "217-898-0185");
    record4.add("address", "green");

    List<Record> records = Arrays.asList(
            record1, record2, record3, record4
    );

    TypeStatistics statistics = new TypeStatistics();
    Record typeRecord = statistics.aggregate(records);
    System.out.println(statistics.typeRecordToStr(typeRecord));
  }
}
