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

import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.TestingRig;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatsTest {

@Test
public void testAggregateStats() throws RecipeException {
	// Create sample data
	List<Row> rows = new ArrayList<>();
	
	// Row 1
	Row row1 = new Row();
	row1.add("data_transfer_size", "10KB");
	row1.add("response_time", "200ms");
	rows.add(row1);
	
	// Row 2
	Row row2 = new Row();
	row2.add("data_transfer_size", "1.5MB");
	row2.add("response_time", "1.2s");
	rows.add(row2);
	
	// Row 3
	Row row3 = new Row();
	row3.add("data_transfer_size", "20KB");
	row3.add("response_time", "500ms");
	rows.add(row3);
	
	// Define recipe
	String[] recipe = new String[] {
	"aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
	};
	
	// Execute recipe
	List<Row> results = null;
	try {
		results = TestingRig.execute(recipe, rows);
	} catch (RecipeException e) {
		e.printStackTrace();
	} catch (DirectiveParseException e) {
		e.printStackTrace();
	} catch (DirectiveLoadException e) {
		e.printStackTrace();
	}
	
	// Ensure results is not null before verifying
	Assert.assertNotNull("Results should not be null", results);
	Assert.assertEquals(1, results.size());
	
	// Calculate expected results
	// 10KB + 1.5MB + 20KB = 1560KB = 1.523MB
	// 200ms + 1.2s + 500ms = 1900ms = 1.9s
	double expectedSizeMB = (10 * 1024 + 1.5 * 1024 * 1024 + 20 * 1024) / (1024.0 * 1024.0);
	double expectedTimeSec = (200 * 1_000_000 + 1.2 * 1_000_000_000 + 500 * 1_000_000) / 1_000_000_000.0;
	
	// Verify calculated values
	Assert.assertEquals(expectedSizeMB, (Double) results.get(0).getValue("total_size_mb"), 0.001);
	Assert.assertEquals(expectedTimeSec, (Double) results.get(0).getValue("total_time_sec"), 0.001);
}

@Test
public void testAggregateStatsEmptyData() throws RecipeException, DirectiveParseException, DirectiveLoadException {
	// Create empty sample data
	List<Row> rows = new ArrayList<>();
	
	// Define recipe
	String[] recipe = new String[] {
	"aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
	};
	
	// Execute recipe
	List<Row> results = TestingRig.execute(recipe, rows);
	
	// Verify results
	Assert.assertEquals(1, results.size());
	Assert.assertEquals(0.0, (Double) results.get(0).getValue("total_size_mb"), 0.001);
	Assert.assertEquals(0.0, (Double) results.get(0).getValue("total_time_sec"), 0.001);
}

@Test
public void testAggregateStatsMissingValues() throws RecipeException, DirectiveParseException, DirectiveLoadException {
	// Create sample data with missing values
	List<Row> rows = new ArrayList<>();
	
	// Row 1 (complete)
	Row row1 = new Row();
	row1.add("data_transfer_size", "10KB");
	row1.add("response_time", "200ms");
	rows.add(row1);
	
	// Row 2 (missing size)
	Row row2 = new Row();
	row2.add("response_time", "1.2s");
	rows.add(row2);
	
	// Row 3 (missing time)
	Row row3 = new Row();
	row3.add("data_transfer_size", "20KB");
	rows.add(row3);
	
	// Define recipe
	String[] recipe = new String[] {
	"aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
	};
	
	// Execute recipe
	List<Row> results = TestingRig.execute(recipe, rows);
	
	// Verify results
	Assert.assertEquals(1, results.size());
	
	// Calculate expected results
	// 10KB + 0 + 20KB = 30KB = 0.029MB
	// 200ms + 1.2s + 0 = 1400ms = 1.4s
	double expectedSizeMB = (10 * 1024 + 20 * 1024) / (1024.0 * 1024.0);
	double expectedTimeSec = (200 * 1_000_000 + 1.2 * 1_000_000_000) / 1_000_000_000.0;
	
	// Verify calculated values
	Assert.assertEquals(expectedSizeMB, (Double) results.get(0).getValue("total_size_mb"), 0.001);
	Assert.assertEquals(expectedTimeSec, (Double) results.get(0).getValue("total_time_sec"), 0.001);
}
}