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
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kewang on 5/25/17.
 */
public class ParseRegex {

  @Ignore
  @Test
  public void run() {
    StringBuilder sb = new StringBuilder();
    List<String> regexs = readFromCsv("zip_code_regex.csv");
    for (String str : regexs) {
      sb.append("<finder>\n" +
              "    <name>Zip_Code</name>\n" +
              "    <pattern>");
      sb.append(str);
      sb.append("</pattern>\n" +
              "    <enabled>true</enabled>\n" +
              "  </finder>\n");
    }
    System.out.println(sb.toString());
  }



  private List<String> readFromCsv(String fileName) {
    List<String> list = new ArrayList<>();
    CSVReader reader = null;
    try {
      BufferedReader bReader = new BufferedReader(new InputStreamReader(
              this.getClass().getResourceAsStream("/" + fileName)));
      reader = new CSVReader(bReader, ',', '"', '\0');
      String [] nextLine;
      while ((nextLine = reader.readNext()) != null) {
        list.add('^' + nextLine[1].substring(1) + '$');
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
