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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.TestUtil;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.statistics.Statistics;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kewang on 5/19/17.
 * TODO: Just a experiment, no use Ken
 */
public class BasicStatisticsTest {
  @Test
  public void testBasicStatistics() throws Exception {

    Record mock1 = new Record();
    //mock1.add("phone", "7-(524)722-4546");
    mock1.add("address1", "478 Macpherson Drive");
    mock1.add("address2", "6707 Eagan Street");
    mock1.add("address3", "59 Farwell Avenue");
    mock1.add("address4", "27472 Bunting Avenue");
    List<Record> records = Arrays.asList(mock1);

    /*
    mock1.add("id", "1");
    mock1.add("first_name", "Paulina");
    mock1.add("last_name", "Wynne");
    mock1.add("phone", "7-(524)722-4546");
    mock1.add("address", "478 Macpherson Drive");
    mock1.add("zip_code", "659600");

    mock1.add("credit_card_number", "5893038608281613");
    mock1.add("VISA_1", "4111111111111111");
    mock1.add("VISA_2","4012888888881881");
    mock1.add("VISA_3","4222222222222");

    mock1.add("MasterCard_1","5555555555554444");
    mock1.add("MasterCard_2","5105105105105100");

    mock1.add ("Discover", "6011 0000 0000 0004");
    mock1.add("AmericanExpress", "3400 0000 0000 009");

    mock1.add("ssn", "723-47-4824");
    mock1.add("ISBN_10", "0-345-50113-6");
    mock1.add("ISBN_13", "978-0-345-50113-4");
    mock1.add("money", "$93110.95");
    mock1.add("latitude", "52.30447");
    mock1.add("longitude", "85.0785");
    mock1.add("IPv4", "84.150.101.246");
    mock1.add("IPv6", "ac68:ff5b:bcdb:e52b:f13d:5642:f97a:3d73");
    mock1.add("MAC address", "B9-27-8E-CE-77-8A");
    mock1.add("other", "荣");

    Record record1 = new Record ("phone", "217-418-5708");
    record1.add("address", "lincoln");

    Record record2 = new Record ("phone", "217-333-1303");
    record2.add("address", "john");

    Record record3 = new Record ("phone", "universe");
    record3.add("address", "217-418-5708");

    Record record4 = new Record ("phone", "217-898-0185");
    record4.add("address", "green");


    List<Record> records = Arrays.asList(
      mock1, mock1, mock1, record1, record2, record3, record4
    );
    */

    Statistics statisticsGen = new BasicStatistics();
    Record summary = statisticsGen.aggregate(records);

    Record stats = (Record) summary.getValue("stats");
    Record types = (Record) summary.getValue("types");

    //System.out.println(types);
    /*
    //TODO: Need fix this Ken Test for type display
    TypeStatistics typeStatistics = new TypeStatistics();
    //System.out.println(typeStatistics.typeRecordToStr(types));
    types = typeStatistics.aggregate(records);
    */

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


    //TODO: test
    //System.out.println(statistics.toString());

    // Final response object.
    JsonObject response = new JsonObject();
    JsonObject result = new JsonObject();

    // Put the statistics along with validation rules.
    result.add("statistics", statistics);
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "Success");
    response.addProperty("count", 2);
    response.add("values", result);

    System.out.println(response.toString());
  }
}
