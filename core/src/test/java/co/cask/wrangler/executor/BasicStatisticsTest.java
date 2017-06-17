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

package co.cask.wrangler.executor;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.api.statistics.Statistics;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.parser.TextDirectives;
import co.cask.wrangler.statistics.BasicStatistics;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link BasicStatistics}
 */
public class BasicStatisticsTest {

  @Test
  public void testMetaBasic() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "drop body"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "1234.45,650-897-3839,111-11-1111,32826,02/29/2000,\"$1234.56\",http://www.yahoo.com"),
      new Record("body", "45.56,670-897-3839,111-12-1111,32826,02/01/2011,\"$56,789\",http://mars.io"),
      new Record("body", "45.56,670-897-3839,222,32826,9/14/2016,\"\",http://mars.io")
    );

    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(new TextDirectives(directives), null);
    records = pipeline.execute(records);

    Statistics meta = new BasicStatistics();
    Record summary = meta.aggregate(records);

    Assert.assertTrue(records.size() > 1);

    Assert.assertEquals(3, summary.length());
    Assert.assertEquals(3.0, summary.getValue("total"));

    Record stats = (Record) summary.getValue("stats");
    Record types = (Record) summary.getValue("types");

    Assert.assertEquals(7, stats.length());
    Assert.assertEquals(7, types.length());

    System.out.println("General Statistics");
    System.out.println();
    List<KeyValue<String, Object>> fields = stats.getFields();
    for (KeyValue<String, Object> field : fields) {
      List<KeyValue<String, Double>> values = (List<KeyValue<String, Double>>) field.getValue();
      for (KeyValue<String, Double> value : values) {
        System.out.println(String.format("%-20s %20s %3.2f%%", field.getKey(), value.getKey(), value.getValue() * 100));
      }
    }

    System.out.println();
    System.out.println("Type Statistics");
    System.out.println();
    fields = types.getFields();
    for (KeyValue<String, Object> field : fields) {
      List<KeyValue<String, Double>> values = (List<KeyValue<String, Double>>) field.getValue();
      for (KeyValue<String, Double> value : values) {
        System.out.println(String.format("%-20s %20s %3.2f%%", field.getKey(), value.getKey(), value.getValue() * 100));
      }
    }
  }

  // Disabled on purpose as we don't want to run this on regular basis.
  @Ignore
  @Test
  public void testLargeFile() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body"
    };

    List<Record> records = new ArrayList<>();
    try(BufferedReader br = new BufferedReader(new FileReader("/Users/nitin/Work/Demo/data/customer_no_header.csv"))) {
      String line;
      while ((line = br.readLine()) != null) {
        records.add(new Record("body", line));
      }
    }

    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(new TextDirectives(directives), null);
    records = pipeline.execute(records);

    Statistics meta = new BasicStatistics();
    Record summary = meta.aggregate(records);

    Record stats = (Record) summary.getValue("stats");
    Record types = (Record) summary.getValue("types");


    System.out.println("General Statistics");
    System.out.println("Total number of records : " + summary.getValue("total"));
    System.out.println();
    List<KeyValue<String, Object>> fields = stats.getFields();
    for (KeyValue<String, Object> field : fields) {
      List<KeyValue<String, Double>> values = (List<KeyValue<String, Double>>) field.getValue();
      for (KeyValue<String, Double> value : values) {
        Double percentage = value.getValue() * 100;
        if(percentage < 20) {
          continue;
        }
        System.out.println(String.format("%10s %-20s %3.2f%%", field.getKey(), value.getKey(), value.getValue() * 100));
      }
    }

    System.out.println();
    System.out.println("Type Statistics");
    System.out.println();
    fields = types.getFields();
    for (KeyValue<String, Object> field : fields) {
      List<KeyValue<String, Double>> values = (List<KeyValue<String, Double>>) field.getValue();
      for (KeyValue<String, Double> value : values) {
        Double percentage = value.getValue() * 100;
        if(percentage < 20) {
          continue;
        }
        System.out.println(String.format("%10s %-20s %3.2f%%", field.getKey(), value.getKey(), value.getValue() * 100));
      }
    }
  }
}

