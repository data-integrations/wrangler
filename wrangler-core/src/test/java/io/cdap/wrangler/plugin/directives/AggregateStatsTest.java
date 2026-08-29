// /*
//  * Copyright © 2017-2019 Cask Data, Inc.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  * use this file except in compliance with the License. You may obtain a copy of
//  * the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  * License for the specific language governing permissions and limitations under
//  * the License.
//  */
// package io.cdap.wrangler.plugin.directives;

// import io.cdap.wrangler.api.Arguments;
// import io.cdap.wrangler.api.ExecutorContext;
// import io.cdap.wrangler.api.Row;
// import io.cdap.wrangler.api.TransientStore;
// import io.cdap.wrangler.api.parser.ColumnName;
// import com.google.gson.JsonElement;
// import org.junit.Before;
// import org.junit.Test;

// import java.util.*;

// import static org.junit.Assert.assertEquals;

// public class AggregateStatsTest {

//   private AggregateStats directive;

//   @Before
//   public void setUp() {
//     directive = new AggregateStats();

//     directive.initialize(new Arguments() {
//       private final Map<String, Object> values = new HashMap<>();

//       {
//         values.put("sizeCol", new ColumnName("bytes"));
//         values.put("timeCol", new ColumnName("duration"));
//         values.put("totalSizeCol", new ColumnName("totalMB"));
//         values.put("totalTimeCol", new ColumnName("totalSecs"));
//       }

//       @Override
//       public Object value(String name) {
//         return values.get(name);
//       }

//       @Override
//       public boolean contains(String name) {
//         return values.containsKey(name);
//       }

//       @Override
//       public boolean has(String name) {
//         return values.containsKey(name);
//       }

//       @Override
//       public <T> T value(String name, Class<T> type) {
//         return type.cast(values.get(name));
//       }

//       @Override
//       public <T> T value(String name, T def, Class<T> type) {
//         return has(name) ? type.cast(values.get(name)) : def;
//       }

//       @Override
//       public ColumnName column(String name) {
//         return (ColumnName) values.get(name);
//       }

//       @Override
//       public JsonElement toJson() {
//         return null;
//       }

//       @Override
//       public String source() {
//         return "test-recipe";
//       }
//     });
//   }

//   @Test
//   public void testExecute() {
//     List<Row> inputRows = new ArrayList<>();

//     Row r1 = new Row();
//     r1.add("bytes", "1MB");
//     r1.add("duration", "2s");

//     Row r2 = new Row();
//     r2.add("bytes", "512KB");
//     r2.add("duration", "500ms");

//     inputRows.add(r1);
//     inputRows.add(r2);

//     ExecutorContext ctx = new ExecutorContext() {
//       @Override
//       public TransientStore getTransientStore() {
//         return null; // we don’t need it for this test
//       }
//     };

//     List<Row> result = directive.execute(inputRows, ctx);

//     assertEquals(1, result.size());
//     Row output = result.get(0);

//     double expectedMB = 1 + 0.5;
//     double expectedSec = 2 + 0.5;

//     assertEquals(expectedMB, (Double) output.getValue("totalMB"), 0.01);
//     assertEquals(expectedSec, (Double) output.getValue("totalSecs"), 0.01);
//   }
// }

