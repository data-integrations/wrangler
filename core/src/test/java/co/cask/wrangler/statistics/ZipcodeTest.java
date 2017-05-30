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

import au.com.bytecode.opencsv.CSVReader;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.TestUtil;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.statistics.Statistics;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test zip code regex detecting. See {@link BasicStatistics} for Regex pattern definitions
 */
public class ZipcodeTest {
  private static final String DATA_FILE = "MOCK_DATA.csv";

  @Test
  public void testZipcode() throws Exception {

    Record mock1 = new Record();

    //US and Mexico
    mock1.add("zip_code_1", "94105-0011");
    mock1.add("zip_code_2", "61801");
    mock1.add("zip_code_3", "61820");

    //China and India
    mock1.add("zip_code_4", "030002");
    mock1.add("zip_code_5", "030001");
    mock1.add("zip_code_6", "382355");

    //Canada
    mock1.add("zip_code_7", "V6T 1Z4");
    mock1.add("zip_code_8", "N2L 3G1");

    //no use
    List<Record> records = Arrays.asList(
            mock1, mock1, mock1
    );
    Statistics statisticsGen = new BasicStatistics();
    Record summary = statisticsGen.aggregate(records);
    Record typesList = (Record) summary.getValue("types");

    for (int i = 0; i < typesList.length(); i ++) {
      ArrayList<KeyValue> types = (ArrayList<KeyValue>) typesList.getValue(i);
      Assert.assertEquals(types.size(), 1);
      Assert.assertEquals(types.get(0).getKey(), "Zip_Code");
    }
  }

  @Test
  public void testNonZipcode() throws Exception {

    List<Record> records = new ArrayList<>();

    List<String> cardNumbers = readCreditCardFromCsv(DATA_FILE);
    for (String cardNumber : cardNumbers) {
      Record record = new Record();
      record.add("card_number", cardNumber);
      records.add(record);
    }

    Statistics statisticsGen = new BasicStatistics();
    Record summary = statisticsGen.aggregate(records);
    Record typesList = (Record) summary.getValue("types");

    for (int i = 0; i < typesList.length(); i ++) {
      ArrayList<KeyValue> types = (ArrayList<KeyValue>) typesList.getValue(i);
      for (KeyValue keyValue : types) {
        /*
        Assert.assertFalse(keyValue.getKey().equals("CN_Code"));
        Assert.assertFalse(keyValue.getKey().equals("IN_Code"));
        Assert.assertFalse(keyValue.getKey().equals("US_Code"));
        Assert.assertFalse(keyValue.getKey().equals("MX_Code"));
        Assert.assertFalse(keyValue.getKey().equals("CA_Code"));
        */
        Assert.assertFalse(keyValue.getKey().equals("Zip_Code"));
      }
    }
  }



  private List<String> readCreditCardFromCsv(String fileName) {
    List<String> list = new ArrayList<>();
    CSVReader reader = null;
    try {
      BufferedReader bReader = new BufferedReader(new InputStreamReader(
              this.getClass().getResourceAsStream("/" + fileName)));
      reader = new CSVReader(bReader);
      String [] nextLine;
      while ((nextLine = reader.readNext()) != null) {
          list.add(nextLine[6]);
      }
      return list;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
