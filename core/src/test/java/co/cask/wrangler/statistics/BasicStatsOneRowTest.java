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

/**
 * Created by kewang on 5/23/17.
 */
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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.statistics.Statistics;
import co.cask.wrangler.api.validator.Validator;
import co.cask.wrangler.api.validator.ValidatorException;
import co.cask.wrangler.validator.ColumnNameValidator;
import com.google.gson.JsonObject;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BasicStatsOneRowTest {
  @Test
  public void testBasicStatistics() throws Exception {

    Record mock1 = new Record();
    mock1.add("phone", "7-(524)722-4546");
    mock1.add("address", "478 Macpherson Drive");
    mock1.add("zip_code", "659600");
    List<Record> records = Arrays.asList(mock1);


    // Final response object.
    JsonObject response = new JsonObject();
    JsonObject result = new JsonObject();

    // Validate Column names.
    Validator<String> validator = new ColumnNameValidator();
    validator.initialize();

    // Iterate through columns to get a set
    Set<String> uniqueColumns = new HashSet<>();
    for (Record record : records) {
      for (int i = 0; i < record.length(); ++i) {
        uniqueColumns.add(record.getColumn(i));
      }
    }

    JsonObject columnValidationResult = new JsonObject();
    for (String name : uniqueColumns) {
      JsonObject columnResult = new JsonObject();
      try {
        validator.validate(name);
        columnResult.addProperty("valid", true);
      } catch (ValidatorException e) {
        columnResult.addProperty("valid", false);
        columnResult.addProperty("message", e.getMessage());
      }
      columnValidationResult.add(name, columnResult);
    }

    result.add("validation", columnValidationResult);

    // Generate General and Type related Statistics for each column.



    Statistics statsGenerator = new BasicStatistics();
    Record summary = statsGenerator.aggregate(records);

    Record stats = (Record) summary.getValue("stats");
    Record types = (Record) summary.getValue("types");

    // Serialize the results into JSON.
    List<KeyValue<String, Object>> fields = stats.getFields();
    JsonObject statistics = new JsonObject();
    for (KeyValue<String, Object> field : fields) {
      List<KeyValue<String, Double>> values = (List<KeyValue<String, Double>>) field.getValue();
      JsonObject v = new JsonObject();
      JsonObject o = new JsonObject();
      for (KeyValue<String, Double> value : values) {
        o.addProperty(value.getKey(), value.getValue().floatValue()*100);
      }
      v.add("general", o);
      statistics.add(field.getKey(), v);
    }

    fields = types.getFields();
    for (KeyValue<String, Object> field : fields) {
      List<KeyValue<String, Double>> values = (List<KeyValue<String, Double>>) field.getValue();
      JsonObject v = new JsonObject();
      JsonObject o = new JsonObject();
      for (KeyValue<String, Double> value : values) {
        o.addProperty(value.getKey(), value.getValue().floatValue()*100);
      }
      v.add("types", o);
      JsonObject object = (JsonObject) statistics.get(field.getKey());
      if (object == null) {
        statistics.add(field.getKey(), v);
      } else {
        object.add("types", o);
      }
    }

    // Put the statistics along with validation rules.
    result.add("statistics", statistics);
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "Success");
    response.addProperty("count", 2);
    response.add("values", result);

    System.out.println(response.toString());
  }


}

