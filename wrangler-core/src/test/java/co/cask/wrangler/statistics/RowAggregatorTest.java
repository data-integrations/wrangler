/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.wrangler.statistics;

import co.cask.wrangler.api.Executor;
import co.cask.wrangler.api.GrammarMigrator;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
public class RowAggregatorTest {

  @Test
  public void testStatisticsGeneration() throws Exception {
    String[] recipe = new String[] {
      "parse-as-csv :body ',';",
      "rename :body_1 :fname;",
      "rename :body_2 :lname;",
      "rename :body_3 :age;",
      "set-type :age int;"
    };

    List<Row> rows = new ArrayList<>();
    rows.add(new Row("body", "A1,B1,10"));
    rows.add(new Row("body", "A2,B2,2"));
    rows.add(new Row("body", "A3,B3,56"));

    GrammarMigrator migrator = new MigrateToV2(recipe);
    String migratedRecipe = migrator.migrate();
    RecipeParser parser = new GrammarBasedParser(migratedRecipe, new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry()
    ));

    Aggregator<Row> aggregator = new RowAggregator();
    aggregator.initialize();

    List<Executor> executors = parser.parse();
    executors.add(aggregator);

    for (Executor executor : executors) {
      if (executor instanceof RowAggregator) {
        ((RowAggregator) executor).execute(rows, null);
      } else {
        rows = (List<Row>) executor.execute(rows, null);
      }
    }

    Row stats = aggregator.get(1L);

    for (int i = 0; i < stats.length(); ++i) {
      System.out.println(stats.getColumn(i) + " : " + stats.getValue(i));
    }

    aggregator.destroy();
  }
}